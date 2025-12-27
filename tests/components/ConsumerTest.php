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
        $connection->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $logger = $this->createMock(Logger::class);
        
        $semaphore = $this->createMock(Semaphore::class);
        $semaphore->expects($this->once())
            ->method('release');
        
        $consumer = new Consumer($connection, $routing, $logger, false, $semaphore);
        $consumer->setQueues(['queue' => 'callback']);
        
        // 先获取信号量（模拟已获取状态）
        $this->setInaccessibleProperty($consumer, 'semaphoreAcquired', true);
        
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
        $routing->expects($this->once())
            ->method('declareAll');
        $logger = $this->createSilentLogger();
        
        $semaphore = $this->createMock(Semaphore::class);
        $semaphore->expects($this->once())
            ->method('acquire_wait')
            ->willThrowException(new \RuntimeException('Failed to acquire semaphore'));
        
        $consumer = new Consumer($connection, $routing, $logger, true, $semaphore);
        $consumer->setQueues(['queue' => 'callback']);
        $consumer->setName('test-consumer');
        
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("Failed to acquire semaphore for consumer 'test-consumer'");
        $consumer->consume();
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
}
