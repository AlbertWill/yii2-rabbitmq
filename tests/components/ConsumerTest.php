<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components;

use mikemadisonweb\rabbitmq\components\{
    Consumer, ConsumerInterface, Logger, Routing
};
use mikemadisonweb\rabbitmq\components\semaphore\Semaphore;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\events\RabbitMQConsumerEvent;
use mikemadisonweb\rabbitmq\exceptions\RuntimeException;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use yii\console\Controller;

class ConsumerTest extends TestCase
{
    /**
     * @dataProvider checkConsume
     * @param $queues
     * @param $consumeCount
     */
    public function testConsume($queues, $consumeCount)
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->once())
            ->method('declareAll');
        $logger = $this->createSilentLogger();
        $consumer = new Consumer($connection, $routing, $logger, true);
        if (!empty($queues)) {
            $consumer->setQueues($queues);
        }
        $channel
            ->expects($consumeCount)
            ->method('basic_consume');
        $this->assertSame(Controller::EXIT_CODE_NORMAL, $consumer->consume());
    }

    public function checkConsume() : array
    {
        return [
            [[], $this->never()],
            [['queue' => 'callback'], $this->once()],
            [['queue1' => 'callback1', 'queue2' => 'callback2', 'queue3' => 'callback3'], $this->exactly(3)],
        ];
    }

    public function testNoAutoDeclare()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->never())
            ->method('declareAll');
        $logger = $this->createSilentLogger();
        $consumer = new Consumer($connection, $routing, $logger, false);
        $consumer->setQos(['prefetch_size' => 0, 'prefetch_count' => 0, 'global' => false]);
        $this->assertSame(Controller::EXIT_CODE_NORMAL, $consumer->consume());
    }

    public function testConsumeEvents()
    {
        $queue = 'test-queue';
        $msgBody = 'Test message!';
        $consumerName = 'test';
        $callbackName = 'MockCallback';
        $callback = $this->getMockBuilder(ConsumerInterface::class)
            ->setMockClassName($callbackName)
            ->getMock();
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'host' => 'unreal',
                        ],
                    ],
                    'queues' => [
                        [
                            'name' => $queue,
                        ],
                    ],
                    'consumers' => [
                        [
                            'name' => $consumerName,
                            'callbacks' => [$queue => $callbackName],
                        ]
                    ],
                    'on before_consume' => function ($event) use ($msgBody) {
                        $this->assertInstanceOf(RabbitMQConsumerEvent::class, $event);
                        $this->assertSame($msgBody, $event->message->getBody());
                    },
                    'on after_consume' => function ($event) use ($msgBody) {
                        $this->assertInstanceOf(RabbitMQConsumerEvent::class, $event);
                        $this->assertSame($msgBody, $event->message->getBody());
                    },
                ],
            ],
        ]);
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $logger = $this->createMock(Logger::class);
        $routing = $this->createMock(Routing::class);
        $consumer = \Yii::$app->rabbitmq->getConsumer($consumerName);
        $this->setInaccessibleProperty($consumer, 'routing', $routing);
        $this->setInaccessibleProperty($consumer, 'conn', $connection);
        $this->setInaccessibleProperty($consumer, 'logger', $logger);
        $msg = new AMQPMessage($msgBody);
        $this->invokeMethod($consumer, 'onReceive', [$msg, $queue, [$callback, 'execute']]);
    }

    public function testOnReceive()
    {
        $queue = 'test-queue';
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'host' => 'unreal',
                        ],
                    ],
                ],
            ],
        ]);
        $callback = $this->createMock(ConsumerInterface::class);
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->never())
            ->method('declareAll');
        $logger = $this->createMock(Logger::class);
        $consumer = $this->getMockBuilder(Consumer::class)
            ->setConstructorArgs([$connection, $routing, $logger, false])
            ->setMethods(['sendResult'])
            ->getMock();
        $msgBody = 'Test message';
        $consumer->method('sendResult')
            ->willThrowException(new \Exception($msgBody));
        $msg = new AMQPMessage($msgBody);
        // No exception should be thrown
        $consumer->setProceedOnException(true);
        $before = $consumer->getConsumed();
        $this->assertTrue($this->invokeMethod($consumer, 'onReceive', [$msg, $queue, [$callback, 'execute']]));
        $this->assertSame($before + 1, $consumer->getConsumed());
        // Exception should be thrown
        $consumer->setProceedOnException(false);
        $this->expectExceptionMessage($msgBody);
        $callback->expects($this->once())
            ->method('execute');
        $logger->expects($this->once())
            ->method('logError');
        $this->invokeMethod($consumer, 'onReceive', [$msg, $queue, [$callback, 'execute']]);
    }

    /**
     * @dataProvider checkMsgTypes
     * @param $userData
     */
    public function testOnReceiveDifferentTypes($userData)
    {
        $queue = 'test-queue';
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'host' => 'unreal',
                        ],
                    ],
                ],
            ],
        ]);
        $callback = $this->createMock(ConsumerInterface::class);
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->never())
            ->method('declareAll');
        $logger = $this->createMock(Logger::class);
        $consumer = $this->getMockBuilder(Consumer::class)
            ->setConstructorArgs([$connection, $routing, $logger, false])
            ->setMethods(['sendResult'])
            ->getMock();
        $consumer->setDeserializer('json_decode');
        $msgBody = json_encode($userData);
        $msg = new AMQPMessage($msgBody);
        $headers['rabbitmq.serialized'] = 1;
        $headersTable = new AMQPTable($headers);
        $msg->set('application_headers', $headersTable);
        $this->invokeMethod($consumer, 'onReceive', [$msg, $queue, [$callback, 'execute']]);
        $this->assertEquals($userData, $msg->getBody());
    }

    public function checkMsgTypes() : array
    {
        return [
            ['String!'],
            [['array']],
            [1],
            [1.1],
            [null],
            [new \StdClass()],
        ];
    }

    public function testForceStop()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $channel->expects($this->once())
            ->method('basic_cancel');
        // 模拟已经注册了消费者（有 callbacks）
        $channel->callbacks = ['queue-unnamed-123' => 'callback'];
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->never())
            ->method('declareAll');
        $logger = $this->createMock(Logger::class);
        $consumer = $this->getMockBuilder(Consumer::class)
            ->setConstructorArgs([$connection, $routing, $logger, false])
            ->setMethods(['maybeStopConsumer'])
            ->getMock();
        $consumer->setQueues(['queue' => 'callback']);
        // 先调用 getChannel() 来设置 $this->ch，确保检查逻辑能正确工作
        $consumer->getChannel();
        // 模拟已经启动了消费
        $this->setInaccessibleProperty($consumer, 'consumingStarted', true);
        $consumer->stopDaemon();
    }

    public function testForceRestart()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $logger = $this->createMock(Logger::class);
        $consumer = $this->getMockBuilder(Consumer::class)
            ->setConstructorArgs([$connection, $routing, $logger, false])
            ->setMethods(['stopConsuming', 'renew', 'setup'])
            ->getMock();
        $consumer->expects($this->once())
            ->method('stopConsuming');
        $consumer->expects($this->once())
            ->method('renew');
        $consumer->expects($this->once())
            ->method('setup');
        $consumer->setQueues(['queue' => 'callback']);
        $consumer->restartDaemon();
    }

    public function testConsumeWithoutSemaphore()
    {
        // 测试向后兼容：无 semaphore 时正常消费
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->once())
            ->method('declareAll');
        $logger = $this->createSilentLogger();
        // 不传入 semaphore（null）
        $consumer = new Consumer($connection, $routing, $logger, true, null);
        $consumer->setQueues(['queue' => 'callback']);
        $channel->expects($this->once())
            ->method('basic_consume');
        $this->assertSame(Controller::EXIT_CODE_NORMAL, $consumer->consume());
    }

    public function testConsumeWithSemaphoreAcquire()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->once())
            ->method('declareAll');
        $logger = $this->createSilentLogger();
        
        $semaphore = $this->createMock(Semaphore::class);
        $semaphore->expects($this->once())
            ->method('acquire_wait')
            ->willReturn(true);
        
        $consumer = new Consumer($connection, $routing, $logger, true, $semaphore);
        $consumer->setQueues(['queue' => 'callback']);
        $channel->expects($this->once())
            ->method('basic_consume');
        $this->assertSame(Controller::EXIT_CODE_NORMAL, $consumer->consume());
    }

    public function testConsumeWithSemaphoreRelease()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->once())
            ->method('declareAll');
        $logger = $this->createSilentLogger();
        
        $semaphore = $this->createMock(Semaphore::class);
        $semaphore->expects($this->once())
            ->method('acquire_wait')
            ->willReturn(true);
        $semaphore->expects($this->once())
            ->method('release');
        
        $consumer = new Consumer($connection, $routing, $logger, true, $semaphore);
        $consumer->setQueues(['queue' => 'callback']);
        $channel->expects($this->once())
            ->method('basic_consume');
        // consume() 会在 finally 块中释放信号量
        $this->assertSame(Controller::EXIT_CODE_NORMAL, $consumer->consume());
    }

    public function testStopDaemonReleasesSemaphore()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $channel->expects($this->once())
            ->method('basic_cancel');
        // 模拟已经注册了消费者（有 callbacks）
        $channel->callbacks = ['queue-unnamed-123' => 'callback'];
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $logger = $this->createMock(Logger::class);
        
        $semaphore = $this->createMock(Semaphore::class);
        $semaphore->expects($this->once())
            ->method('release');
        
        $consumer = new Consumer($connection, $routing, $logger, false, $semaphore);
        $consumer->setQueues(['queue' => 'callback']);
        
        // 先调用 getChannel() 来设置 $this->ch，确保检查逻辑能正确工作
        $consumer->getChannel();
        
        // 先获取信号量（模拟已获取状态）
        $this->setInaccessibleProperty($consumer, 'semaphoreAcquired', true);
        // 模拟已经启动了消费
        $this->setInaccessibleProperty($consumer, 'consumingStarted', true);
        
        $consumer->stopDaemon();
    }

    public function testRestartDaemonKeepsSemaphore()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $logger = $this->createMock(Logger::class);
        
        $semaphore = $this->createMock(Semaphore::class);
        // restartDaemon 不应该释放信号量
        $semaphore->expects($this->never())
            ->method('release');
        
        $consumer = $this->getMockBuilder(Consumer::class)
            ->setConstructorArgs([$connection, $routing, $logger, false, $semaphore])
            ->setMethods(['stopConsuming', 'renew', 'setup'])
            ->getMock();
        $consumer->expects($this->once())
            ->method('stopConsuming');
        $consumer->expects($this->once())
            ->method('renew');
        $consumer->expects($this->once())
            ->method('setup');
        $consumer->setQueues(['queue' => 'callback']);
        
        // 先获取信号量（模拟已获取状态）
        $this->setInaccessibleProperty($consumer, 'semaphoreAcquired', true);
        
        $consumer->restartDaemon();
    }

    public function testWaitForMessageHeartbeat()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $logger = $this->createMock(Logger::class);
        
        $semaphore = $this->createMock(Semaphore::class);
        $semaphore->expects($this->once())
            ->method('heartbeat');
        
        $consumer = new Consumer($connection, $routing, $logger, false, $semaphore);
        $consumer->setIdleTimeout(1);
        
        // 先获取信号量（模拟已获取状态）
        $this->setInaccessibleProperty($consumer, 'semaphoreAcquired', true);
        
        // 模拟 wait() 正常返回
        $channel->expects($this->once())
            ->method('wait')
            ->willReturn(null);
        
        $result = $this->invokeMethod($consumer, 'waitForMessage', []);
        $this->assertFalse($result['needContinue']);
        $this->assertNull($result['exitCode']);
    }

    public function testWaitForMessageTimeoutHeartbeat()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $logger = $this->createMock(Logger::class);
        
        $semaphore = $this->createMock(Semaphore::class);
        $semaphore->expects($this->once())
            ->method('heartbeat');
        
        $consumer = new Consumer($connection, $routing, $logger, false, $semaphore);
        $consumer->setIdleTimeout(1);
        // 不设置 idleTimeoutExitCode，所以超时后 needContinue 应该保持为 true
        
        // 先获取信号量（模拟已获取状态）
        $this->setInaccessibleProperty($consumer, 'semaphoreAcquired', true);
        
        // 模拟 wait() 超时
        $channel->expects($this->once())
            ->method('wait')
            ->willThrowException(new AMQPTimeoutException('Timeout'));
        
        $result = $this->invokeMethod($consumer, 'waitForMessage', []);
        // 当超时且没有设置 idleTimeoutExitCode 时，needContinue 应该保持为 true（默认值）
        $this->assertTrue($result['needContinue']);
        $this->assertNull($result['exitCode']);
    }

    public function testSemaphoreAcquireFailure()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        // 信号量获取被信号中断时，setup() 不会被执行，所以 declareAll() 不应该被调用
        $routing->expects($this->never())
            ->method('declareAll');
        $logger = $this->createSilentLogger();
        
        $semaphore = $this->createMock(Semaphore::class);
        $semaphore->expects($this->once())
            ->method('acquire_wait')
            ->willReturn(false); // 返回 false 表示 sleep() 被信号中断
        
        $consumer = new Consumer($connection, $routing, $logger, true, $semaphore);
        $consumer->setQueues(['queue' => 'callback']);
        $consumer->setName('test-consumer');
        
        // acquire_wait() 返回 false 时，acquireSemaphore() 会检查 forceStop
        // 如果 forceStop 为 true，会返回 false，consume() 会直接返回 ExitCode::OK
        // 这里模拟信号中断的情况，设置 forceStop 为 true
        $this->setInaccessibleProperty($consumer, 'forceStop', true);
        
        // 应该优雅退出，不抛出异常
        $result = $consumer->consume();
        $this->assertEquals(Controller::EXIT_CODE_NORMAL, $result);
    }

    public function testSemaphoreReleaseFailure()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $logger = $this->createMock(Logger::class);
        
        $semaphore = $this->createMock(Semaphore::class);
        $semaphore->expects($this->once())
            ->method('release')
            ->willThrowException(new \RuntimeException('Failed to release semaphore'));
        
        $consumer = new Consumer($connection, $routing, $logger, false, $semaphore);
        $consumer->setQueues(['queue' => 'callback']);
        
        // 先获取信号量（模拟已获取状态）
        $this->setInaccessibleProperty($consumer, 'semaphoreAcquired', true);
        
        // release 失败不应该抛出异常，只记录日志
        $consumer->stopDaemon();
    }

    public function testSemaphoreHeartbeatFailure()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $logger = $this->createMock(Logger::class);
        
        $semaphore = $this->createMock(Semaphore::class);
        $semaphore->expects($this->once())
            ->method('heartbeat')
            ->willThrowException(new \RuntimeException('Failed to heartbeat'));
        
        $consumer = new Consumer($connection, $routing, $logger, false, $semaphore);
        $consumer->setIdleTimeout(1);
        
        // 先获取信号量（模拟已获取状态）
        $this->setInaccessibleProperty($consumer, 'semaphoreAcquired', true);
        
        // 模拟 wait() 正常返回
        $channel->expects($this->once())
            ->method('wait')
            ->willReturn(null);
        
        // heartbeat 失败不应该抛出异常，只记录日志
        $result = $this->invokeMethod($consumer, 'waitForMessage', []);
        $this->assertFalse($result['needContinue']);
    }

    public function testConsumeExceptionReleasesSemaphore()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->once())
            ->method('declareAll');
        $logger = $this->createMock(Logger::class);
        
        $semaphore = $this->createMock(Semaphore::class);
        $semaphore->expects($this->once())
            ->method('acquire_wait')
            ->willReturn(true);
        $semaphore->expects($this->once())
            ->method('release'); // 即使抛出异常，finally 块也应该释放
        
        $consumer = new Consumer($connection, $routing, $logger, true, $semaphore);
        $consumer->setQueues(['queue' => 'callback']);
        
        // 模拟 waitForMessage 内部抛出异常（通过 wait 抛出异常）
        // waitForMessage 会捕获异常并重新抛出，导致 consume 循环退出
        $channel->expects($this->once())
            ->method('wait')
            ->willThrowException(new \Exception('Unexpected error'));
        
        // 设置 callbacks 不为空，让循环继续
        $channel->callbacks = ['callback1'];
        
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Unexpected error');
        
        try {
            $consumer->consume();
        } catch (\Exception $e) {
            // 验证 semaphore 已被释放（finally 块应该执行）
            $this->assertFalse($this->getInaccessibleProperty($consumer, 'semaphoreAcquired'));
            throw $e;
        }
    }

    /**
     * 测试连接断开重连 - 验证异常处理和重连逻辑
     * 由于重连逻辑复杂，这里主要测试 waitForMessage 方法对连接异常的处理
     */
    public function testConnectionClosedReconnect()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel', 'reconnect', 'isConnected', 'close'])
            ->getMock();
        
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->setMethods(['wait', 'close', 'getChannelId', 'basic_consume', 'basic_qos'])
            ->getMock();
        
        $connection->method('channel')
            ->willReturn($channel);
        $connection->method('isConnected')
            ->willReturn(true);
        // reconnect 可能内部调用 disableHeartbeat，我们让它返回自身以支持链式调用
        $connection->method('reconnect')
            ->willReturnSelf();
        $connection->method('close')
            ->willReturn(null);
        
        $channel->method('getChannelId')
            ->willReturn(1);
        $channel->method('close')
            ->willReturn(null);
        $channel->method('basic_consume')
            ->willReturn(null);
        $channel->method('basic_qos')
            ->willReturn(null);
        
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->atLeastOnce())
            ->method('declareAll');
        
        $logger = $this->createMock(Logger::class);
        $logger->expects($this->atLeastOnce())
            ->method('logDebug');
        
        $consumer = new Consumer($connection, $routing, $logger, true, null);
        $consumer->setQueues(['queue' => 'callback']);
        $consumer->setMaxReconnectAttempts(3);
        $consumer->setReconnectDelay(0);
        
        // wait 抛出连接异常，重连成功后直接返回（不会再次调用 wait）
        $channel->expects($this->once())
            ->method('wait')
            ->willThrowException(new \PhpAmqpLib\Exception\AMQPConnectionClosedException('Connection closed'));
        
        // 测试 waitForMessage 方法处理连接异常
        // 重连成功后，needContinue 应该是 true，表示需要继续循环（在下一次循环中再次调用 wait）
        $result = $this->invokeMethod($consumer, 'waitForMessage', []);
        $this->assertTrue($result['needContinue'], 'Should continue loop after reconnection (will call wait again in next iteration)');
        $this->assertNull($result['exitCode'], 'Exit code should be null');
    }

    /**
     * 测试通道异常处理
     * 模拟 AMQPChannelClosedException 异常，验证通道重建逻辑
     */
    public function testChannelClosedException()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->setMethods(['wait', 'close', 'getChannelId', 'basic_consume', 'basic_qos'])
            ->getMock();
        
        $connection->method('channel')
            ->willReturn($channel);
        
        $channel->method('getChannelId')
            ->willReturn(1);
        $channel->method('basic_consume')
            ->willReturn(null);
        $channel->method('basic_qos')
            ->willReturn(null);
        
        $routing = $this->createMock(Routing::class);
        // 通道异常时，setup() 会被调用一次（重建通道时）
        $routing->expects($this->once())
            ->method('declareAll');
        
        $logger = $this->createMock(Logger::class);
        $logger->expects($this->atLeastOnce())
            ->method('logDebug');
        
        $consumer = new Consumer($connection, $routing, $logger, true, null);
        $consumer->setQueues(['queue' => 'callback']);
        
        // wait 抛出通道异常，通道重建成功后直接返回（不会再次调用 wait）
        $channel->expects($this->once())
            ->method('wait')
            ->willThrowException(new \PhpAmqpLib\Exception\AMQPChannelClosedException('Channel closed'));
        
        $channel->expects($this->once())
            ->method('close');
        
        $result = $this->invokeMethod($consumer, 'waitForMessage', []);
        $this->assertTrue($result['needContinue'], 'Should continue loop after channel rebuild (will call wait again in next iteration)');
    }

    /**
     * 测试重连失败场景
     * 模拟重连多次失败，验证抛出正确的异常
     */
    public function testReconnectFailure()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel', 'reconnect', 'isConnected', 'close'])
            ->getMock();
        
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->setMethods(['wait', 'close', 'getChannelId', 'basic_consume', 'basic_qos'])
            ->getMock();
        
        $connection->method('channel')
            ->willReturn($channel);
        $connection->method('isConnected')
            ->willReturn(true);
        $connection->method('reconnect')
            ->willThrowException(new \Exception('Reconnect failed'));
        $connection->method('close')
            ->willReturn(null);
        
        $channel->method('getChannelId')
            ->willReturn(1);
        $channel->method('close')
            ->willReturn(null);
        $channel->method('basic_consume')
            ->willReturn(null);
        $channel->method('basic_qos')
            ->willReturn(null);
        
        $routing = $this->createMock(Routing::class);
        $logger = $this->createMock(Logger::class);
        
        $consumer = new Consumer($connection, $routing, $logger, true, null);
        $consumer->setQueues(['queue' => 'callback']);
        $consumer->setMaxReconnectAttempts(2);
        $consumer->setReconnectDelay(0);
        
        // wait 抛出连接异常
        $channel->expects($this->once())
            ->method('wait')
            ->willThrowException(new \PhpAmqpLib\Exception\AMQPConnectionClosedException('Connection closed'));
        
        $this->expectException(\PhpAmqpLib\Exception\AMQPIOException::class);
        $this->expectExceptionMessage('MQ连接重试失败');
        
        $this->invokeMethod($consumer, 'waitForMessage', []);
    }

    /**
     * 测试 idle timeout 处理
     * 验证 waitForMessage 正确处理超时
     */
    public function testIdleTimeoutHandling()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->setMethods(['wait'])
            ->getMock();
        
        $connection->method('channel')
            ->willReturn($channel);
        
        $routing = $this->createMock(Routing::class);
        $logger = $this->createMock(Logger::class);
        
        $consumer = new Consumer($connection, $routing, $logger, false, null);
        $consumer->setIdleTimeout(1);
        $consumer->setIdleTimeoutExitCode(100);
        
        // wait 抛出超时异常
        $channel->expects($this->once())
            ->method('wait')
            ->willThrowException(new AMQPTimeoutException('Idle timeout'));
        
        $result = $this->invokeMethod($consumer, 'waitForMessage', []);
        
        $this->assertFalse($result['needContinue'], 'Should not continue when exit code is set');
        $this->assertEquals(100, $result['exitCode'], 'Should return configured exit code');
    }

    /**
     * 测试 idle timeout 无退出码
     * 验证当没有设置 idleTimeoutExitCode 时，超时后继续循环
     */
    public function testIdleTimeoutWithoutExitCode()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->setMethods(['wait'])
            ->getMock();
        
        $connection->method('channel')
            ->willReturn($channel);
        
        $routing = $this->createMock(Routing::class);
        $logger = $this->createMock(Logger::class);
        
        $consumer = new Consumer($connection, $routing, $logger, false, null);
        $consumer->setIdleTimeout(1);
        $consumer->setIdleTimeoutExitCode(null);
        
        // wait 抛出超时异常
        $channel->expects($this->once())
            ->method('wait')
            ->willThrowException(new AMQPTimeoutException('Idle timeout'));
        
        $result = $this->invokeMethod($consumer, 'waitForMessage', []);
        
        $this->assertTrue($result['needContinue'], 'Should continue when no exit code is set');
        $this->assertNull($result['exitCode'], 'Exit code should be null');
    }

    /**
     * 测试重连时信号量保持
     * 验证重连过程中信号量不被释放
     */
    public function testSemaphorePreservedDuringReconnect()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel', 'reconnect', 'isConnected', 'close'])
            ->getMock();
        
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->setMethods(['wait', 'close', 'getChannelId', 'basic_consume', 'basic_qos'])
            ->getMock();
        
        $connection->method('channel')
            ->willReturn($channel);
        $connection->method('isConnected')
            ->willReturn(true);
        $connection->method('reconnect')
            ->willReturnSelf();
        $connection->method('close')
            ->willReturn(null);
        
        $channel->method('getChannelId')
            ->willReturn(1);
        $channel->method('close')
            ->willReturn(null);
        $channel->method('basic_consume')
            ->willReturn(null);
        $channel->method('basic_qos')
            ->willReturn(null);
        
        $routing = $this->createMock(Routing::class);
        $logger = $this->createMock(Logger::class);
        
        $semaphore = $this->createMock(Semaphore::class);
        // acquire_wait 在 consume() 方法中调用，不在 waitForMessage 中
        $semaphore->expects($this->never())
            ->method('acquire_wait');
        // 重连时不应该释放信号量
        $semaphore->expects($this->never())
            ->method('release');
        // 重连成功后应该刷新心跳
        $semaphore->expects($this->once())
            ->method('heartbeat');
        
        $consumer = new Consumer($connection, $routing, $logger, true, $semaphore);
        $consumer->setQueues(['queue' => 'callback']);
        $consumer->setMaxReconnectAttempts(3);
        $consumer->setReconnectDelay(0);
        
        // 先获取信号量（模拟已获取状态）
        $this->setInaccessibleProperty($consumer, 'semaphoreAcquired', true);
        
        // wait 抛出连接异常，重连成功后直接返回（不会再次调用 wait）
        $channel->expects($this->once())
            ->method('wait')
            ->willThrowException(new \PhpAmqpLib\Exception\AMQPConnectionClosedException('Connection closed'));
        
        // 模拟 setup 方法（重连后会调用一次）
        $routing->expects($this->once())
            ->method('declareAll');
        
        $result = $this->invokeMethod($consumer, 'waitForMessage', []);
        $this->assertTrue($result['needContinue'], 'Should continue loop after reconnection (will call wait again in next iteration)');
        
        // 验证信号量仍然被持有
        $this->assertTrue($this->getInaccessibleProperty($consumer, 'semaphoreAcquired'), 'Semaphore should still be acquired');
    }

    /**
     * 测试多种连接异常类型
     * 验证不同类型的连接异常都能触发重连
     */
    public function testVariousConnectionExceptions()
    {
        $exceptions = [
            new \PhpAmqpLib\Exception\AMQPConnectionClosedException('Connection closed'),
            new \PhpAmqpLib\Exception\AMQPDataReadException('Data read error'),
            new \PhpAmqpLib\Exception\AMQPIOException('IO error'),
            new \PhpAmqpLib\Exception\AMQPBasicCancelException('Basic cancel'),
        ];
        
        foreach ($exceptions as $exception) {
            $connection = $this->getMockBuilder(AMQPLazyConnection::class)
                ->disableOriginalConstructor()
                ->setMethods(['channel', 'reconnect', 'isConnected', 'close'])
                ->getMock();
            
            $channel = $this->getMockBuilder(AMQPChannel::class)
                ->disableOriginalConstructor()
                ->setMethods(['wait', 'close', 'getChannelId', 'basic_consume', 'basic_qos'])
                ->getMock();
            
            $connection->method('channel')
                ->willReturn($channel);
            $connection->method('isConnected')
                ->willReturn(true);
            $connection->method('reconnect')
                ->willReturnSelf();
            $connection->method('close')
                ->willReturn(null);
            
            $channel->method('getChannelId')
                ->willReturn(1);
            $channel->method('close')
                ->willReturn(null);
            $channel->method('basic_consume')
                ->willReturn(null);
            $channel->method('basic_qos')
                ->willReturn(null);
            
            $routing = $this->createMock(Routing::class);
            $logger = $this->createMock(Logger::class);
            
            $consumer = new Consumer($connection, $routing, $logger, true, null);
            $consumer->setQueues(['queue' => 'callback']);
            $consumer->setMaxReconnectAttempts(3);
            $consumer->setReconnectDelay(0);
            
            // wait 抛出异常，重连成功后直接返回（不会再次调用 wait）
            $channel->expects($this->once())
                ->method('wait')
                ->willThrowException($exception);
            
            // 重连后会调用 setup，setup 会调用 declareAll
            $routing->expects($this->once())
                ->method('declareAll');
            
            $result = $this->invokeMethod($consumer, 'waitForMessage', []);
            $this->assertTrue($result['needContinue'], 'Should continue loop after handling ' . get_class($exception) . ' (will call wait again in next iteration)');
        }
    }
}
