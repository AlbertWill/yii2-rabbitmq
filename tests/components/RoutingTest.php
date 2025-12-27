<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components;

use mikemadisonweb\rabbitmq\exceptions\RuntimeException;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;

class RoutingTest extends TestCase
{
    public function testRouting()
    {
        $name = 'test';
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'name' => $name,
                            'url' => 'amqp://user:pass@host:5432/vhost?query',
                        ],
                    ],
                    'exchanges' => [
                        [
                            'name' => $name,
                            'type' => 'direct'
                        ],
                    ],
                    'queues' => [
                        [
                            'name' => $name,
                            'durable' => true,
                        ],
                        [
                            'durable' => false,
                        ],
                    ],
                    'bindings' => [
                        [
                            'queue' => $name,
                            'exchange' => $name,
                            'routing_keys' => [$name],
                        ],
                        [
                            'exchange' => $name,
                            'to_exchange' => $name,
                            'routing_keys' => [$name],
                        ],
                        [
                            'queue' => $name,
                            'exchange' => $name,
                        ],
                        [
                            'exchange' => $name,
                            'to_exchange' => $name,
                        ],
                        [
                            'queue' => '',
                            'exchange' => $name,
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
        $connection->method('channel')
            ->willReturn($channel);
        // 设置连接的 name 属性
        $connection->name = $name;
        $routing = \Yii::$app->rabbitmq->getRouting($connection);
        $this->assertTrue($routing->declareAll());
        $this->assertFalse($routing->declareAll());
    }

    /**
     * @dataProvider checkExceptions
     * @param $functionName
     */
    public function testRoutingExceptions($functionName)
    {
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'url' => 'amqp://user:pass@host:5432/vhost?query',
                        ],
                    ],
                ],
            ],
        ]);
        $connection = \Yii::$app->rabbitmq->getConnection();
        $routing = \Yii::$app->rabbitmq->getRouting($connection);
        $this->expectException(RuntimeException::class);
        $routing->$functionName('non-existing');
    }

    /**
     * @return array
     */
    public function checkExceptions() : array
    {
        return [
            ['declareQueue'],
            ['declareExchange'],
            ['purgeQueue'],
            ['deleteQueue'],
            ['deleteExchange'],
        ];
    }

    public function testRoutingNonExisting()
    {
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'url' => 'amqp://user:pass@host:5432/vhost?query',
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
        $exception = $this->createMock(AMQPProtocolChannelException::class);
        $channel
            ->expects($this->once())
            ->method('exchange_declare')
            ->willThrowException($exception);
        $channel
            ->expects($this->once())
            ->method('queue_declare')
            ->willThrowException($exception);
        $connection->method('channel')
            ->willReturn($channel);
        // 设置连接的 name 属性
        $connection->name = Configuration::DEFAULT_CONNECTION_NAME;
        $routing = \Yii::$app->rabbitmq->getRouting($connection);
        $this->assertFalse($routing->isExchangeExists('non-existing'));
        $this->assertFalse($routing->isQueueExists('non-existing'));
    }

    public function testRoutingExisting()
    {
        $name = 'test';
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'url' => 'amqp://user:pass@host:5432/vhost?query',
                        ],
                    ],
                    'exchanges' => [
                        [
                            'name' => $name,
                            'type' => 'direct'
                        ],
                    ],
                    'queues' => [
                        [
                            'name' => $name,
                            'durable' => true,
                        ],
                        [
                            'durable' => false,
                        ],
                    ],
                    'producers' => [
                        [
                            'name' => $name,
                            'connection' => Configuration::DEFAULT_CONNECTION_NAME,
                        ],
                    ],
                    'bindings' => [
                        [
                            'queue' => $name,
                            'exchange' => $name,
                            'routing_keys' => [$name],
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
        $channel
            ->expects($this->once())
            ->method('exchange_declare');
        $channel
            ->expects($this->once())
            ->method('queue_declare');
        $connection->method('channel')
            ->willReturn($channel);
        // 设置连接的 name 属性
        $connection->name = Configuration::DEFAULT_CONNECTION_NAME;
        $routing = \Yii::$app->rabbitmq->getRouting($connection);
        $this->assertTrue($routing->isExchangeExists($name));
        $this->assertTrue($routing->isQueueExists($name));
        // Test purging queue - 需要先声明队列
        $channel
            ->expects($this->once())
            ->method('queue_purge');
        $routing->purgeQueue($name);
        // Test deleting all schema
        // 由于只有 1 个队列在 producers/consumers 中使用，所以只有 1 个队列会被删除
        // 由于 exchange 在 producers/consumers 中使用，所以会被删除
        $channel
            ->expects($this->once())
            ->method('queue_delete');
        $channel
            ->expects($this->once())
            ->method('exchange_delete');
        $routing->deleteAll();
    }
}
