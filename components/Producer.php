<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\components;

use mikemadisonweb\rabbitmq\DependencyInjection;
use mikemadisonweb\rabbitmq\events\RabbitMQPublisherEvent;
use mikemadisonweb\rabbitmq\exceptions\RuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * Service that sends AMQP Messages
 *
 * @package mikemadisonweb\rabbitmq\components
 */
class Producer extends BaseRabbitMQ
{
    protected $contentType;

    protected $deliveryMode;

    protected $serializer;

    protected $safe;

    protected $name = 'unnamed';

    //重连尝试次数
    protected $maxReconnectAttempts = 3;
    //重连休息秒数
    protected $reconnectDelay = 2;

    /**
     * @param $contentType
     */
    public function setContentType($contentType)
    {
        $this->contentType = $contentType;
    }

    /**
     * @param $deliveryMode
     */
    public function setDeliveryMode($deliveryMode)
    {
        $this->deliveryMode = $deliveryMode;
    }

    /**
     * @param callable $serializer
     */
    public function setSerializer(callable $serializer)
    {
        $this->serializer = $serializer;
    }

    /**
     * @return callable
     */
    public function getSerializer(): callable
    {
        return $this->serializer;
    }

    /**
     * @return array
     */
    public function getBasicProperties(): array
    {
        return [
            'content_type'  => $this->contentType,
            'delivery_mode' => $this->deliveryMode,
        ];
    }

    /**
     * @return mixed
     */
    public function getSafe(): bool
    {
        return $this->safe;
    }

    /**
     * @param mixed $safe
     */
    public function setSafe(bool $safe)
    {
        $this->safe = $safe;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @param string $name
     */
    public function setName(string $name)
    {
        $this->name = $name;
    }

    /**
     * publish 的入口（覆盖父类方法）
     * 捕获常见连接/通道/IO 异常，触发重连并在重连成功后自动重试一次。
     * @param mixed $msgBody
     * @param string $exchangeName
     * @param string $routingKey
     * @param array $additionalProperties
     * @param array|null $headers
     * @throws \Throwable
     */
    public function publish($msgBody, string $exchangeName, string $routingKey = '', array $additionalProperties = [], array $headers = null)
    {
        try {
            //调用父类方法publish消息
            $this->execPublish($msgBody, $exchangeName, $routingKey, $additionalProperties, $headers);

            return;
        } catch (\Throwable $e) {
            //判断异常是否为可恢复的连接
            if(!$this->isRecoverable($e)){
                throw $e;
            }
        }

        try {
            // 重连
            $this->handleReconnect($e);

            // 重连成功后重试一次
            $this->execPublish($msgBody, $exchangeName, $routingKey, $additionalProperties, $headers);

            return;
        } catch (\Throwable $e2) {
            // 重试失败，则抛出异常
            throw $e2;
        }
    }

    /**
     * Publishes the message and merges additional properties with basic properties
     *
     * @param mixed  $msgBody
     * @param string $exchangeName
     * @param string $routingKey
     * @param array  $additionalProperties
     * @param array  $headers
     *
     * @throws RuntimeException
     */
    public function execPublish(
        $msgBody,
        string $exchangeName,
        string $routingKey = '',
        array $additionalProperties = [],
        array $headers = null
    ) {
        if ($this->autoDeclare)
        {
            $this->routing->declareAll();
        }
        if ($this->safe && !$this->routing->isExchangeExists($exchangeName))
        {
            throw new RuntimeException(
                "Exchange `{$exchangeName}` does not declared in broker (You see this message because safe mode is ON)."
            );
        }
        $serialized = false;
        if (!is_string($msgBody))
        {
            $msgBody    = call_user_func($this->serializer, $msgBody);
            $serialized = true;
        }
        $msg = new AMQPMessage($msgBody, array_merge($this->getBasicProperties(), $additionalProperties));

        if (!empty($headers) || $serialized)
        {
            if ($serialized)
            {
                $headers['rabbitmq.serialized'] = 1;
            }
            $headersTable = new AMQPTable($headers);
            $msg->set('application_headers', $headersTable);
        }

        \Yii::$app->rabbitmq->trigger(
            RabbitMQPublisherEvent::BEFORE_PUBLISH,
            new RabbitMQPublisherEvent(
                [
                    'message'  => $msg,
                    'producer' => $this,
                ]
            )
        );

        $this->getChannel()->basic_publish($msg, $exchangeName, $routingKey);

        \Yii::$app->rabbitmq->trigger(
            RabbitMQPublisherEvent::AFTER_PUBLISH,
            new RabbitMQPublisherEvent(
                [
                    'message'  => $msg,
                    'producer' => $this,
                ]
            )
        );

        $this->logger->log(
            'AMQP message published',
            $msg,
            [
                'exchange'    => $exchangeName,
                'routing_key' => $routingKey,
            ]
        );
    }

    /**
     * 判断异常是否为可恢复的连接/通道/IO 类型异常
     * @param \Throwable $e
     * @return bool
     */
    protected function isRecoverable(\Throwable $e): bool
    {
        // 类类型判断（常见的 PhpAmqpLib 异常）
        $recoverableClasses = [
            \PhpAmqpLib\Exception\AMQPHeartbeatMissedException::class,
            \PhpAmqpLib\Exception\AMQPConnectionClosedException::class,
            \PhpAmqpLib\Exception\AMQPChannelClosedException::class,
            \PhpAmqpLib\Exception\AMQPIOException::class,
            \PhpAmqpLib\Exception\AMQPConnectionBlockedException::class,
            \PhpAmqpLib\Exception\AMQPDataReadException::class,
        ];

        foreach ($recoverableClasses as $cls) {
            if ($e instanceof $cls) {
                return true;
            }
        }

        // 关键字兜底（Broken pipe / Socket closed / Packets out of order 等）
        $msg = $e->getMessage();
        $keywords = [
            'Broken pipe',
            'Socket closed',
            'Packets out of order',
            'Connection reset by peer',
            'write ECONNRESET',
            'timed out',
            'READ_ERROR',
        ];
        foreach ($keywords as $k) {
            if (stripos($msg, $k) !== false) {
                return true;
            }
        }

        return false;
    }

    /**
     * 执行重连逻辑（带防重入锁）
     *
     * - 关闭旧连接（尝试）
     * - 调用 renew() 建立新连接
     * - 重建 channel 并重新声明交换机/队列（如果 autoDeclare）
     *
     * 最终若重连失败，会抛出 RuntimeException 并附带原始异常
     *
     * @param \Throwable $origEx 原始触发重连的异常
     * @throws RuntimeException
     */
    protected function handleReconnect(\Throwable $origEx): void
    {
        $name = $this->conn->name;

        for ($i = 1; $i <= $this->maxReconnectAttempts; $i++) {
            try {
                $this->forceCloseConnection();

                $this->conn = DependencyInjection::renewConnection($name);

                $this->ch   = $this->conn->channel();

                // 重建 routing
                $this->routing = new Routing($this->conn);

                return;

            } catch (\Throwable $ex) {
                sleep($this->reconnectDelay);
            }
        }

        throw new \RuntimeException("RabbitMQ重连失败 {$this->maxReconnectAttempts} 次", 0, $origEx);
    }

    /**
     * 尝试优雅关闭旧连接（忽略关闭过程中的异常）
     */
    protected function forceCloseConnection(): void
    {
        try {
            if ($this->ch && method_exists($this->ch, 'close')) {
                try { $this->ch->close(); } catch (\Throwable $t) {}
            }
        } catch (\Throwable $t) {}

        try {
            if ($this->conn && method_exists($this->conn, 'close')) {
                try { $this->conn->close(); } catch (\Throwable $t) {}
            }
        } catch (\Throwable $t) {}
        // 强制销毁资源
        $this->conn = null;
        $this->ch   = null;
    }

}
