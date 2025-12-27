<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\components;

use BadFunctionCallException;
use ErrorException;
use mikemadisonweb\rabbitmq\components\semaphore\Semaphore;
use mikemadisonweb\rabbitmq\events\RabbitMQConsumerEvent;
use mikemadisonweb\rabbitmq\exceptions\RuntimeException;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Exception\AMQPBasicCancelException;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPDataReadException;
use PhpAmqpLib\Exception\AMQPIOException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Message\AMQPMessage;
use Throwable;
use yii\console\ExitCode;

/**
 * Service that receives AMQP Messages
 *
 * @package mikemadisonweb\rabbitmq\components
 */
class Consumer extends BaseRabbitMQ
{
    protected $deserializer;

    protected $qos;

    protected $idleTimeout;

    protected $idleTimeoutExitCode;

    protected $queues = [];

    protected $memoryLimit = 0;

    protected $proceedOnException;

    protected $name = 'unnamed';

    private $id;

    private $target;

    private $consumed = 0;

    private $forceStop = false;

    /** @var int 重试计数 */
    private $reconnectAttempts = 0;

    /** @var int 最大连接重试次数 */
    protected $maxReconnectAttempts;

    /** @var int 重试间隔（秒） */
    protected $reconnectDelay;

    /** @var Semaphore|null 信号量实例 */
    protected $semaphore;

    /** @var bool 是否已获取信号量 */
    private $semaphoreAcquired = false;

    /**
     * @param AbstractConnection $conn
     * @param Routing $routing
     * @param Logger $logger
     * @param bool $autoDeclare
     * @param Semaphore|null $semaphore
     */
    public function __construct(AbstractConnection $conn, Routing $routing, Logger $logger, bool $autoDeclare, ?Semaphore $semaphore = null)
    {
        parent::__construct($conn, $routing, $logger, $autoDeclare);
        $this->semaphore = $semaphore;
    }

    /**
     * Set the memory limit
     *
     * @param int $memoryLimit
     */
    public function setMemoryLimit($memoryLimit)
    {
        $this->memoryLimit = $memoryLimit;
    }

    /**
     * Get the memory limit
     *
     * @return int
     */
    public function getMemoryLimit(): int
    {
        return $this->memoryLimit;
    }

    /**
     * @param array $queues
     */
    public function setQueues(array $queues)
    {
        $this->queues = $queues;
    }

    /**
     * @return array
     */
    public function getQueues(): array
    {
        return $this->queues;
    }

    /**
     * @param $idleTimeout
     */
    public function setIdleTimeout($idleTimeout)
    {
        $this->idleTimeout = $idleTimeout;
    }

    public function getIdleTimeout()
    {
        return $this->idleTimeout;
    }

    /**
     * Set exit code to be returned when there is a timeout exception
     *
     * @param int|null $idleTimeoutExitCode
     */
    public function setIdleTimeoutExitCode($idleTimeoutExitCode)
    {
        $this->idleTimeoutExitCode = $idleTimeoutExitCode;
    }

    /**
     * Get exit code to be returned when there is a timeout exception
     *
     * @return int|null
     */
    public function getIdleTimeoutExitCode()
    {
        return $this->idleTimeoutExitCode;
    }

    /**
     * @return mixed
     */
    public function getDeserializer(): callable
    {
        return $this->deserializer;
    }

    /**
     * @param mixed $deserializer
     */
    public function setDeserializer(callable $deserializer)
    {
        $this->deserializer = $deserializer;
    }

    /**
     * @return mixed
     */
    public function getQos(): array
    {
        return $this->qos;
    }

    /**
     * @param mixed $qos
     */
    public function setQos(array $qos)
    {
        $this->qos = $qos;
    }

    /**
     * @param string $name
     */
    public function setName(string $name)
    {
        $this->name = $name;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * Resets the consumed property.
     * Use when you want to call start() or consume() multiple times.
     */
    public function getConsumed(): int
    {
        return $this->consumed;
    }

    /**
     * Resets the consumed property.
     * Use when you want to call start() or consume() multiple times.
     */
    public function resetConsumed()
    {
        $this->consumed = 0;
    }

    /**
     * @return mixed
     */
    public function getProceedOnException(): bool
    {
        return $this->proceedOnException;
    }

    /**
     * @param mixed $proceedOnException
     */
    public function setProceedOnException(bool $proceedOnException)
    {
        $this->proceedOnException = $proceedOnException;
    }

    /**
     * @return int
     */
    public function getMaxReconnectAttempts(): int
    {
        return $this->maxReconnectAttempts;
    }

    /**
     * @param int $maxReconnectAttempts
     */
    public function setMaxReconnectAttempts(int $maxReconnectAttempts): void
    {
        $this->maxReconnectAttempts = $maxReconnectAttempts;
    }

    /**
     * @return int
     */
    public function getReconnectDelay(): int
    {
        return $this->reconnectDelay;
    }

    /**
     * @param int $reconnectDelay
     */
    public function setReconnectDelay(int $reconnectDelay): void
    {
        $this->reconnectDelay = $reconnectDelay;
    }

    /**
     * Consume designated number of messages (0 means infinite)
     *
     * 信号量生命周期说明：
     * - 信号量在消费者启动时（setup() 之后）获取，在整个消费生命周期内保持
     * - 只在消费者进程真正停止前释放：stopDaemon()（信号触发）、循环正常退出（finally 块）
     * - 通过 semaphoreAcquired 标志避免重复释放
     * - 在 wait() 正常返回或超时后调用 heartbeat() 刷新信号量 TTL，防止长时间无消息时信号量过期
     * - restartDaemon() 重启消费时保持信号量（不释放也不重新获取）
     *
     * @param int $msgAmount
     *
     * @return int
     * @throws BadFunctionCallException
     * @throws RuntimeException
     * @throws AMQPTimeoutException
     * @throws ErrorException
     */
    public function consume($msgAmount = 0): int
    {
        $this->target = $msgAmount;
        $this->setup();

        // 在循环外获取信号量，在整个消费生命周期内保持
        $this->acquireSemaphore();

        try {
            while (count($this->getChannel()->callbacks)) {

                $this->logger->logDebug("消费消息开始=========>Start");

                if ($this->maybeStopConsumer()) {
                    break;
                }

                // 等待消息并处理异常
                $result = $this->waitForMessage();
                // 指定了退出码，直接退出进程
                if ($result['exitCode'] !== null) {
                    return $result['exitCode'];
                }
                // 需要继续循环
                if ($result['needContinue']) {
                    continue;
                }

                if (!AMQP_WITHOUT_SIGNALS && extension_loaded('pcntl')) {

                    $this->logger->logDebug("调用 pcntl_signal_dispatch");

                    pcntl_signal_dispatch();
                }

                $this->logger->logDebug("消费消息完成=============OK");

            }
        } finally {
            // 循环退出时统一释放信号量（通过检查 semaphoreAcquired 避免重复释放）
            $this->releaseSemaphore();
        }

        return ExitCode::OK;
    }

    /**
     * Stop consuming messages
     * 停止消费消息（取消订阅），不释放信号量
     */
    public function stopConsuming()
    {
        foreach ($this->queues as $name => $options)
        {
            $this->getChannel()->basic_cancel($this->getConsumerTag($name), false, true);
        }
    }

    /**
     * Force stop the consumer
     * 停止消费者并释放信号量
     */
    public function stopDaemon()
    {
        $this->forceStop = true;
        $this->stopConsuming();
        // 释放信号量
        $this->releaseSemaphore();

        $this->logger->printInfo("\nConsumer stopped by user.\n");
    }

    /**
     * Force restart the consumer
     * 重启消费（停止消费、重建连接、重新设置），保持信号量
     */
    public function restartDaemon()
    {
        $this->stopConsuming();
        $this->renew();
        $this->setup();
        // 信号量保持（因为 stopConsuming() 不再释放信号量）
        $this->logger->printInfo("\nConsumer has been restarted.\n");
    }

    /**
     * Sets the qos settings for the current channel
     * This method needs a connection to broker
     */
    protected function setQosOptions()
    {
        if (empty($this->qos))
        {
            return;
        }
        $prefetchSize  = $this->qos['prefetch_size'] ?? null;
        $prefetchCount = $this->qos['prefetch_count'] ?? null;
        $global        = $this->qos['global'] ?? null;
        $this->getChannel()->basic_qos($prefetchSize, $prefetchCount, $global);
    }

    /**
     * Start consuming messages
     *
     * @throws RuntimeException
     */
    protected function startConsuming()
    {
        $this->id = $this->generateUniqueId();
        foreach ($this->queues as $queue => $callback)
        {
            $that = $this;
            $this->getChannel()->basic_consume(
                $queue,
                $this->getConsumerTag($queue),
                null,
                null,
                null,
                null,
                function (AMQPMessage $msg) use ($that, $queue, $callback)
                {
                    // Execute user-defined callback
                    $that->onReceive($msg, $queue, $callback);
                }
            );
        }
    }

    /**
     * Decide whether it's time to stop consuming
     *
     * @throws BadFunctionCallException
     */
    protected function maybeStopConsumer(): bool
    {

        $this->logger->logDebug("maybeStopConsumer=======Maybe");

        if (extension_loaded('pcntl') && (defined('AMQP_WITHOUT_SIGNALS') ? !AMQP_WITHOUT_SIGNALS : true))
        {
            if (!function_exists('pcntl_signal_dispatch'))
            {
                throw new BadFunctionCallException(
                    "Function 'pcntl_signal_dispatch' is referenced in the php.ini 'disable_functions' and can't be called."
                );
            }
            pcntl_signal_dispatch();
        }
        if ($this->forceStop || ($this->consumed === $this->target && $this->target > 0))
        {
            $this->stopConsuming();

            return true;
        }

        if (0 !== $this->getMemoryLimit() && $this->isRamAlmostOverloaded())
        {
            $this->stopConsuming();

            return true;
        }

        return false;
    }

    /**
     * Callback that will be fired upon receiving new message
     *
     * @param AMQPMessage $msg
     * @param             $queueName
     * @param             $callback
     *
     * @return bool
     * @throws Throwable
     */
    protected function onReceive(AMQPMessage $msg, string $queueName, callable $callback): bool
    {
        $timeStart = microtime(true);
        \Yii::$app->rabbitmq->trigger(
            RabbitMQConsumerEvent::BEFORE_CONSUME,
            new RabbitMQConsumerEvent(
                [
                    'message'  => $msg,
                    'consumer' => $this,
                ]
            )
        );

        try
        {
            // deserialize message back to initial data type
            if ($msg->has('application_headers') &&
                isset($msg->get('application_headers')->getNativeData()['rabbitmq.serialized']))
            {
                $msg->setBody(call_user_func($this->deserializer, $msg->getBody()));
            }
            // process message and return the result code back to broker
            $processFlag = $callback($msg);
            $this->sendResult($msg, $processFlag);
            \Yii::$app->rabbitmq->trigger(
                RabbitMQConsumerEvent::AFTER_CONSUME,
                new RabbitMQConsumerEvent(
                    [
                        'message'  => $msg,
                        'consumer' => $this,
                    ]
                )
            );

            $this->logger->printResult($queueName, $processFlag, $timeStart);
            $this->logger->log(
                'Queue message processed.',
                $msg,
                [
                    'queue'       => $queueName,
                    'processFlag' => $processFlag,
                    'timeStart'   => $timeStart,
                    'memory'      => true,
                ]
            );
        }
        catch (Throwable $e)
        {
            $this->logger->logError($e, $msg);
            if (!$this->proceedOnException)
            {
                throw $e;
            }
        }
        $this->consumed++;

        return true;
    }

    /**
     * Mark message status based on return code from callback
     *
     * @param AMQPMessage $msg
     * @param             $processFlag
     */
    protected function sendResult(AMQPMessage $msg, $processFlag)
    {
        // true in testing environment
        if (!isset($msg->delivery_info['channel']))
        {
            return;
        }

        // respond to the broker with appropriate reply code
        if ($processFlag === ConsumerInterface::MSG_REQUEUE || false === $processFlag)
        {
            // Reject and requeue message to RabbitMQ
            $msg->delivery_info['channel']->basic_reject($msg->delivery_info['delivery_tag'], true);
        }
        elseif ($processFlag === ConsumerInterface::MSG_REJECT)
        {
            // Reject and drop
            $msg->delivery_info['channel']->basic_reject($msg->delivery_info['delivery_tag'], false);
        }
        else
        {
            // Remove message from queue only if callback return not false
            $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
        }
    }

    /**
     * Checks if memory in use is greater or equal than memory allowed for this process
     *
     * @return boolean
     */
    protected function isRamAlmostOverloaded(): bool
    {
        return memory_get_usage(true) >= ($this->getMemoryLimit() * 1024 * 1024);
    }

    /**
     * @param string $queueName
     *
     * @return string
     */
    protected function getConsumerTag(string $queueName): string
    {
        return sprintf('%s-%s-%s', $queueName, $this->name, $this->id);
    }

    /**
     * @return string
     */
    protected function generateUniqueId(): string
    {
        return uniqid('rabbitmq_', true);
    }

    protected function setup()
    {
        $this->resetConsumed();
        if ($this->autoDeclare)
        {
            $this->routing->declareAll();
        }
        $this->setQosOptions();
        $this->startConsuming();
    }

    /**
     * mq连接断开异常处理
     * 注意：重连时信号量保持不变（信号量基于 Redis，不依赖于 RabbitMQ 连接）
     * 如果重连时间较长，信号量可能会过期，但 wait() 超时后会调用 heartbeat() 刷新 TTL
     * 
     * @param $e
     * @throws AMQPIOException
     */
    private function mqClosedException($e): void
    {
        for ($this->reconnectAttempts = 1; $this->reconnectAttempts <= $this->maxReconnectAttempts; $this->reconnectAttempts++) {
            try {
                //重建mq连接
                $this->renew();
                //重建channel通道
                $this->setup();
                $this->reconnectAttempts = 0;
                $this->logger->logDebug("连接重建成功");
                // 重连成功后刷新信号量 TTL，防止重连期间信号量过期
                $this->heartbeatSemaphore();
                return;
            } catch (\Exception $reopenEx) {
                $this->logger->logDebug("重建连接失败: {$reopenEx->getMessage()}");

                if ($this->reconnectAttempts < $this->maxReconnectAttempts) {
                    sleep($this->reconnectDelay); // 等待再试
                }
            }
        }

        // 抛出包含原始异常的新异常
        throw new AMQPIOException("MQ连接重试失败（尝试次数: {$this->maxReconnectAttempts})", 0, $e);
    }

    /**
     * mq通道异常处理
     * 注意：通道重建时信号量保持不变（信号量基于 Redis，不依赖于 RabbitMQ 通道）
     * 
     * @param $e
     */
    private function channelClosedException($e): void
    {
        // Channel 级别错误
        try {
            $this->getChannel()->close();
        } catch (\Throwable $closeEx) {
            $this->logger->logDebug("关闭通道时发生异常: {$closeEx->getMessage()}");
        }

        //重建channel通道
        try {
            $this->logger->logDebug("开始重建通道...");
            $this->setup();
            $this->logger->logDebug("通道重建成功！");
            // 通道重建成功后刷新信号量 TTL（虽然通道重建通常很快，但为保险起见）
            $this->heartbeatSemaphore();
        } catch (\Exception $chanEx) {
            $this->logger->logDebug("通道重建失败: {$chanEx->getMessage()}");
            throw $e;
        }

    }

    /**
     * 等待消息并处理异常
     * 封装 wait() 操作及其异常处理逻辑
     *
     * @return array{needContinue: bool, exitCode: int|null} 返回是否继续循环和退出码
     *   - needContinue: true 表示需要继续循环（跳过后续代码），false 表示继续执行后续代码
     *   - exitCode: 如果指定了退出码则返回退出码，否则为 null
     * @throws \Exception 其他异常直接抛出
     */
    private function waitForMessage(): array
    {
        $needContinue = true;  // 默认需要 continue（异常情况跳过后续代码）
        $exitCode = null;

        try {
            $this->getChannel()->wait(null, false, $this->getIdleTimeout());

            // wait() 正常返回后调用心跳刷新信号量 TTL
            $this->heartbeatSemaphore();
            // 正常返回时，不需要 continue，继续执行后续代码
            $needContinue = false;
        } catch (AMQPTimeoutException $e) {
            $this->logger->logDebug("idle超时[" . get_class($e) . "]:" . $e->getMessage());

            // wait() 超时后调用心跳刷新信号量 TTL
            $this->heartbeatSemaphore();

            // 指定了退出码，返回退出码（不继续循环）
            if (null !== $this->getIdleTimeoutExitCode()) {
                $needContinue = false;
                $exitCode = $this->getIdleTimeoutExitCode();
            }
        } catch (AMQPConnectionClosedException | AMQPDataReadException | AMQPIOException | AMQPBasicCancelException $e) {
            $this->logger->logDebug("连接断开[" . get_class($e) . "]:" . $e->getMessage());

            //mq连接异常处理
            $this->mqClosedException($e);
        } catch (AMQPProtocolChannelException | AMQPChannelClosedException $e) {
            $this->logger->logDebug("通道异常[" . get_class($e) . "]:" . $e->getMessage());

            //mq通道异常处理
            $this->channelClosedException($e);
        } catch (\Exception $e) {
            $this->logger->logDebug("捕获异常[" . get_class($e) . "]:" . $e->getMessage());

            throw $e;
        }

        return ['needContinue' => $needContinue, 'exitCode' => $exitCode];
    }

    /**
     * 获取信号量
     * 在消费者启动时调用，在整个消费生命周期内保持信号量
     *
     * @throws \Exception 获取失败时抛出异常
     */
    private function acquireSemaphore(): void
    {
        if ($this->semaphore === null || $this->semaphoreAcquired) {
            return;
        }

        try {
            $this->logger->logDebug("尝试获取并发名额...");
            $this->semaphore->acquire_wait();
            $this->semaphoreAcquired = true;
            $this->logger->logDebug("成功获取并发名额");
        } catch (\Exception $e) {
            $this->logger->logDebug("获取并发名额失败[" . get_class($e) . "]:" . $e->getMessage());
            // 获取失败时确保状态一致
            $this->semaphoreAcquired = false;
            // 抛出更详细的异常信息，包含消费者名称和信号量配置信息
            throw new RuntimeException(
                "Failed to acquire semaphore for consumer '{$this->name}': " . $e->getMessage(),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * 释放信号量
     * 在消费者进程停止前调用，通过检查 semaphoreAcquired 避免重复释放
     */
    private function releaseSemaphore(): void
    {
        if ($this->semaphore === null || !$this->semaphoreAcquired) {
            return;
        }

        $this->logger->logDebug("释放并发名额");
        try {
            $this->semaphore->release();
        } catch (\Exception $e) {
            $this->logger->logDebug("释放并发名额失败[" . get_class($e) . "]:" . $e->getMessage());
        }
        $this->semaphoreAcquired = false;
    }

    /**
     * 心跳刷新信号量 TTL
     * 在 wait() 超时或正常返回后调用，防止长时间无消息时信号量过期
     * 心跳失败不影响主流程，只记录日志
     */
    private function heartbeatSemaphore(): void
    {
        if ($this->semaphore === null || !$this->semaphoreAcquired) {
            return;
        }

        try {
            $this->semaphore->heartbeat();
        } catch (\Exception $e) {
            $this->logger->logDebug("心跳刷新失败[" . get_class($e) . "]:" . $e->getMessage());
            // 心跳失败不影响主流程，继续执行
        }
    }
}

