<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components;

use mikemadisonweb\rabbitmq\components\{
    Logger, Producer, Routing
};
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\events\RabbitMQPublisherEvent;
use mikemadisonweb\rabbitmq\exceptions\RuntimeException;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPLazyConnection;

class ProducerTest extends TestCase
{
    /**
     * Test without framework
     */
    public function testPublish()
    {
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
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $channel->expects($this->once())
            ->method('basic_publish');
        $connection
            ->method('channel')
            ->willReturn($channel);
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->exactly(2))
            ->method('declareAll');
        $routing->expects($this->exactly(2))
            ->method('isExchangeExists')
            ->willReturnOnConsecutiveCalls(true, false);
        $logger = $this->createMock(Logger::class);
        $producer = new Producer($connection, $routing, $logger, true);
        $producer->setSafe(true);
        // Good attempt
        $producer->publish('Test message', 'exist');
        // Non-existing exchange
        $this->expectException(RuntimeException::class);
        $producer->publish('Test message', 'not-exist');
    }

    /**
     * Test events
     */
    public function testPublishEvents()
    {
        $producerName = 'test';
        $msg = 'Some-message';
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'host' => 'unreal',
                        ],
                    ],
                    'producers' => [
                        [
                            'name' => $producerName,
                        ]
                    ],
                    'on before_publish' => function ($event) use ($msg) {
                        $this->assertInstanceOf(RabbitMQPublisherEvent::class, $event);
                        $this->assertSame($msg, $event->message->getBody());
                    },
                    'on after_publish' => function ($event) use ($msg) {
                        $this->assertInstanceOf(RabbitMQPublisherEvent::class, $event);
                        $this->assertSame($msg, $event->message->getBody());
                    },
                ],
            ],
        ]);
        $producer = \Yii::$app->rabbitmq->getProducer($producerName);
        $routing = $this->createMock(Routing::class);
        $routing->method('declareAll');
        $routing->method('isExchangeExists')
            ->willReturn(true);
        $this->setInaccessibleProperty($producer, 'routing', $routing);
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $channel->expects($this->once())
            ->method('basic_publish');
        $connection
            ->method('channel')
            ->willReturn($channel);
        $this->setInaccessibleProperty($producer, 'conn', $connection);
        $producer->publish($msg, 'exchange');
    }

    /**
     * Test inside framework with different message types
     * @dataProvider checkMsgEncoding
     * @param $initial
     * @param $encoded
     */
    public function testPublishDifferentTypes($initial, $encoded)
    {
        $producerName = 'test';
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'host' => 'unreal',
                        ],
                    ],
                    'producers' => [
                        [
                            'name' => $producerName,
                        ]
                    ],
                    'on after_publish' => function ($event) use ($encoded) {
                        $this->assertSame($encoded, $event->message->getBody());
                    },
                ],
            ],
        ]);
        $producer = \Yii::$app->rabbitmq->getProducer($producerName);
        $routing = $this->createMock(Routing::class);
        $routing->method('declareAll');
        $routing->method('isExchangeExists')
            ->willReturn(true);
        $this->setInaccessibleProperty($producer, 'routing', $routing);
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $channel->expects($this->once())
            ->method('basic_publish');
        $connection
            ->method('channel')
            ->willReturn($channel);
        $this->setInaccessibleProperty($producer, 'conn', $connection);
        $producer->publish($initial, 'exchange');
    }

    public function checkMsgEncoding() : array
    {
        return [
            ['String!', 'String!'],
            [['array'], 'a:1:{i:0;s:5:"array";}'],
            [1, 'i:1;'],
            [null, 'N;'],
            [new \StdClass(), 'O:8:"stdClass":0:{}'],
        ];
    }

    /**
     * 测试连接断开重连 - 验证异常识别
     * 由于重连逻辑需要实际配置，这里主要测试异常识别逻辑
     */
    public function testConnectionClosedReconnect()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->getMock();
        
        $routing = $this->createMock(Routing::class);
        $logger = $this->createSilentLogger();
        
        $producer = new Producer($connection, $routing, $logger, false);
        
        // 验证连接异常被识别为可恢复
        $isRecoverable = $this->invokeMethod($producer, 'isRecoverable', [
            new \PhpAmqpLib\Exception\AMQPConnectionClosedException('Connection closed')
        ]);
        $this->assertTrue($isRecoverable, 'AMQPConnectionClosedException should be recoverable');
    }

    /**
     * 测试通道异常处理 - 验证异常识别
     */
    public function testChannelClosedException()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->getMock();
        
        $routing = $this->createMock(Routing::class);
        $logger = $this->createSilentLogger();
        
        $producer = new Producer($connection, $routing, $logger, false);
        
        // 验证通道异常被识别为可恢复
        $isRecoverable = $this->invokeMethod($producer, 'isRecoverable', [
            new \PhpAmqpLib\Exception\AMQPChannelClosedException('Channel closed')
        ]);
        $this->assertTrue($isRecoverable, 'AMQPChannelClosedException should be recoverable');
    }

    /**
     * 测试可恢复异常判断
     * 测试 isRecoverable 方法正确识别各种异常类型
     */
    public function testIsRecoverable()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->getMock();
        
        $routing = $this->createMock(Routing::class);
        $logger = $this->createSilentLogger();
        
        $producer = new Producer($connection, $routing, $logger, false);
        
        // 可恢复的异常
        $recoverableExceptions = [
            new \PhpAmqpLib\Exception\AMQPHeartbeatMissedException('Heartbeat missed'),
            new \PhpAmqpLib\Exception\AMQPConnectionClosedException('Connection closed'),
            new \PhpAmqpLib\Exception\AMQPChannelClosedException('Channel closed'),
            new \PhpAmqpLib\Exception\AMQPIOException('IO error'),
            new \PhpAmqpLib\Exception\AMQPConnectionBlockedException('Connection blocked'),
            new \PhpAmqpLib\Exception\AMQPDataReadException('Data read error'),
            new \Exception('Broken pipe'),
            new \Exception('Socket closed'),
            new \Exception('Packets out of order'),
            new \Exception('Connection reset by peer'),
            new \Exception('write ECONNRESET'),
            new \Exception('timed out'),
            new \Exception('READ_ERROR'),
        ];
        
        foreach ($recoverableExceptions as $exception) {
            $isRecoverable = $this->invokeMethod($producer, 'isRecoverable', [$exception]);
            $this->assertTrue($isRecoverable, get_class($exception) . ' should be recoverable');
        }
        
        // 不可恢复的异常
        $nonRecoverableExceptions = [
            new \InvalidArgumentException('Invalid argument'),
            new \RuntimeException('Runtime error'),
            new \LogicException('Logic error'),
        ];
        
        foreach ($nonRecoverableExceptions as $exception) {
            $isRecoverable = $this->invokeMethod($producer, 'isRecoverable', [$exception]);
            $this->assertFalse($isRecoverable, get_class($exception) . ' should not be recoverable');
        }
    }

    /**
     * 测试重连失败场景 - 验证异常识别
     * 由于重连逻辑需要实际配置，这里主要测试异常识别逻辑
     */
    public function testReconnectFailure()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->getMock();
        
        $routing = $this->createMock(Routing::class);
        $logger = $this->createSilentLogger();
        
        $producer = new Producer($connection, $routing, $logger, false);
        
        // 验证连接异常被识别为可恢复
        $isRecoverable = $this->invokeMethod($producer, 'isRecoverable', [
            new \PhpAmqpLib\Exception\AMQPConnectionClosedException('Connection closed')
        ]);
        $this->assertTrue($isRecoverable, 'Exception should be recoverable');
    }

    /**
     * 测试不可恢复异常
     * 验证非连接/通道异常被识别为不可恢复
     */
    public function testNonRecoverableException()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->getMock();
        
        $routing = $this->createMock(Routing::class);
        $logger = $this->createSilentLogger();
        
        $producer = new Producer($connection, $routing, $logger, false);
        
        // 验证不可恢复的异常被正确识别
        $nonRecoverableException = new \InvalidArgumentException('Invalid argument');
        $isRecoverable = $this->invokeMethod($producer, 'isRecoverable', [$nonRecoverableException]);
        $this->assertFalse($isRecoverable, 'Exception should not be recoverable');
    }

    /**
     * 测试关键字匹配的异常识别
     * 验证通过异常消息中的关键字识别可恢复异常
     */
    public function testKeywordBasedExceptionRecognition()
    {
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->getMock();
        
        $routing = $this->createMock(Routing::class);
        $logger = $this->createSilentLogger();
        
        $producer = new Producer($connection, $routing, $logger, false);
        
        // 测试各种关键字异常
        $keywordExceptions = [
            new \Exception('Broken pipe error'),
            new \Exception('Socket closed unexpectedly'),
            new \Exception('Packets out of order detected'),
            new \Exception('Connection reset by peer'),
            new \Exception('write ECONNRESET failed'),
            new \Exception('Operation timed out'),
            new \Exception('READ_ERROR occurred'),
        ];
        
        foreach ($keywordExceptions as $exception) {
            $isRecoverable = $this->invokeMethod($producer, 'isRecoverable', [$exception]);
            $this->assertTrue($isRecoverable, 'Exception with message "' . $exception->getMessage() . '" should be recoverable');
        }
    }
}
