<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests;

use mikemadisonweb\rabbitmq\components\{
    Consumer, ConsumerInterface, Logger, Producer, Routing
};
use mikemadisonweb\rabbitmq\components\semaphore\HashSemaphore;
use mikemadisonweb\rabbitmq\components\semaphore\IncrSemaphore;
use mikemadisonweb\rabbitmq\components\semaphore\Semaphore;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\controllers\RabbitMQController;
use mikemadisonweb\rabbitmq\exceptions\InvalidConfigException;
use PhpAmqpLib\Connection\AbstractConnection;
use yii\redis\Connection;

class DependencyInjectionTest extends TestCase
{
    public function testBootstrap()
    {
        $name = 'test';
        $callbackName = 'CallbackMock';
        $this->getMockBuilder(ConsumerInterface::class)
            ->setMockClassName($callbackName)
            ->getMock();
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
                    ],
                    'bindings' => [
                        [
                            'queue' => $name,
                            'exchange' => $name,
                            'routing_keys' => [$name],
                        ],
                    ],
                    'producers' => [
                        [
                            'name' => $name,
                            'connection' => $name,
                        ],
                    ],
                    'consumers' => [
                        [
                            'name' => $name,
                            'connection' => $name,
                            'callbacks' => [
                                $name => $callbackName,
                            ],
                        ],
                    ],
                ],
            ],
        ]);
        $container = \Yii::$container;
        $connection = $container->get(sprintf(Configuration::CONNECTION_SERVICE_NAME, $name));
        $this->assertInstanceOf(AbstractConnection::class, $connection);
        $this->assertInstanceOf(Routing::class, $container->get(sprintf(Configuration::ROUTING_SERVICE_NAME, $name), ['conn' => $connection]));
        $this->assertInstanceOf(Producer::class, $container->get(sprintf(Configuration::PRODUCER_SERVICE_NAME, $name)));
        $this->assertInstanceOf(Consumer::class, $container->get(sprintf(Configuration::CONSUMER_SERVICE_NAME, $name)));
        $this->assertInstanceOf(Logger::class, $container->get(Configuration::LOGGER_SERVICE_NAME));
        $this->assertSame(\Yii::$app->controllerMap[Configuration::EXTENSION_CONTROLLER_ALIAS], RabbitMQController::class);
    }

    public function testBootstrapEmpty()
    {
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'host' => 'somehost',
                        ],
                    ],
                ],
            ],
        ]);
        $container = \Yii::$container;
        $conn = $container->get(sprintf(Configuration::CONNECTION_SERVICE_NAME, Configuration::DEFAULT_CONNECTION_NAME));
        $this->assertInstanceOf(AbstractConnection::class, $conn);
        $router = $container->get(sprintf(Configuration::ROUTING_SERVICE_NAME, Configuration::DEFAULT_CONNECTION_NAME), ['conn' => $conn]);
        // Declare nothing as nothing was configured
        $this->assertTrue($router->declareAll($conn));
    }

    public function testBootstrapWrongUrl()
    {
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'url' => 'https://www.rabbitmq.com/uri-spec.html',
                        ],
                    ],
                ],
            ],
        ]);
        $this->expectException(\InvalidArgumentException::class);
        \Yii::$app->rabbitmq->getConnection();
    }

    public function testBootstrapProducer()
    {
        $producerName = 'smth';
        $contentType = 'non-existing';
        $deliveryMode = 432;
        $serializer = 'json_encode';
        $safe = false;
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
                            'content_type' => $contentType,
                            'delivery_mode' => $deliveryMode,
                            'safe' => $safe,
                            'serializer' => $serializer,
                        ]
                    ],
                ],
            ],
        ]);
        // Test producer setter injection
        $producer = \Yii::$container->get(sprintf(Configuration::PRODUCER_SERVICE_NAME, $producerName));
        $props = $producer->getBasicProperties();
        $this->assertSame($producerName, $producer->getName());
        $this->assertSame($safe, $producer->getSafe());
        $this->assertSame($contentType, $props['content_type']);
        $this->assertSame($deliveryMode, $props['delivery_mode']);
        $this->assertSame($serializer, $producer->getSerializer());
    }

    public function testBootstrapConsumer()
    {
        $consumerName = 'smth';
        $queueName = 'non-existing';
        $callbackName = 'CallbackMock';
        $callback = $this->getMockBuilder(ConsumerInterface::class)
            ->setMockClassName($callbackName)
            ->setMethods(['execute'])
            ->getMock();
        $deserializer = 'json_decode';
        $qos = [
            'prefetch_size' => 11,
            'prefetch_count' => 11,
            'global' => true,
        ];
        $idleTimeout = 100;
        $idleTimeoutExitCode = 101;
        $proceedOnException = true;
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
                            'name' => $queueName,
                        ]
                    ],
                    'consumers' => [
                        [
                            'name' => $consumerName,
                            'callbacks' => [
                                $queueName => $callbackName,
                            ],
                            'qos' => $qos,
                            'idle_timeout' => $idleTimeout,
                            'idle_timeout_exit_code' => $idleTimeoutExitCode,
                            'proceed_on_exception' => $proceedOnException,
                            'deserializer' => $deserializer,
                        ]
                    ],
                ],
            ],
        ]);
        $consumer = \Yii::$container->get(sprintf(Configuration::CONSUMER_SERVICE_NAME, $consumerName));
        $this->assertSame($consumerName, $consumer->getName());
        $this->assertSame(array_keys([$queueName => $callback,]), array_keys($consumer->getQueues()));
        $this->assertSame($qos, $consumer->getQos());
        $this->assertSame($idleTimeout, $consumer->getIdleTimeout());
        $this->assertSame($idleTimeoutExitCode, $consumer->getIdleTimeoutExitCode());
        $this->assertSame($deserializer, $consumer->getDeserializer());
        $this->assertSame($proceedOnException, $consumer->getProceedOnException());
    }

    public function testBootstrapLogger()
    {
        $options = [
            'log' => true,
            'category' => 'some',
            'print_console' => false,
            'system_memory' => true,
        ];
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'host' => 'unreal',
                        ],
                    ],
                    'logger' => $options,
                ],
            ],
        ]);
        $logger = \Yii::$container->get(Configuration::LOGGER_SERVICE_NAME);
        $this->assertSame($options, $logger->options);
    }

    public function testValidateCallbackInterface()
    {
        $callbackAlias = 'callback_mock';
        $callbackName = 'WrongCallbackMock';
        $queueName = 'queue';
        $consumerName = 'consumer';
        // not implementing interface
        $this
            ->getMockBuilder(\AnotherInterface::class)
            ->setMockClassName($callbackName)
            ->disableOriginalConstructor()
            ->getMock();
        \Yii::$container->setSingleton($callbackAlias, ['class' => $callbackName]);
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
                            'name' => $queueName,
                        ]
                    ],
                    'consumers' => [
                        [
                            'name' => $consumerName,
                            'callbacks' => [
                                $queueName => $callbackAlias,
                            ],
                        ]
                    ],
                ],
            ],
        ]);
        $this->expectException(InvalidConfigException::class);
        \Yii::$app->rabbitmq->getConsumer($consumerName);
    }

    public function testBootstrapConsumerWithSemaphore()
    {
        $consumerName = 'test-consumer';
        $queueName = 'test-queue';
        $callbackName = 'CallbackMock';
        $callback = $this->getMockBuilder(ConsumerInterface::class)
            ->setMockClassName($callbackName)
            ->setMethods(['execute'])
            ->getMock();
        
        // 创建 Redis mock
        $redis = $this->createMock(Connection::class);
        
        $this->loadExtension([
            'id' => 'testapp',
            'components' => [
                'redis' => $redis,
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'host' => 'unreal',
                        ],
                    ],
                    'queues' => [
                        [
                            'name' => $queueName,
                        ]
                    ],
                    'semaphore' => [
                        'type' => HashSemaphore::class,
                        'redis_component_name' => 'redis',
                        'limit' => 10,
                        'ttl' => 300,
                        'acquire_sleep' => 60,
                    ],
                    'consumers' => [
                        [
                            'name' => $consumerName,
                            'callbacks' => [
                                $queueName => $callbackName,
                            ],
                            'semaphore' => [
                                'limit' => 5, // Consumer 特定配置覆盖全局配置
                            ],
                        ]
                    ],
                ],
            ],
        ]);
        $consumer = \Yii::$container->get(sprintf(Configuration::CONSUMER_SERVICE_NAME, $consumerName));
        $this->assertInstanceOf(Consumer::class, $consumer);
        
        // 验证 semaphore 已注入
        $semaphore = $this->getInaccessibleProperty($consumer, 'semaphore');
        $this->assertInstanceOf(Semaphore::class, $semaphore);
        $this->assertInstanceOf(HashSemaphore::class, $semaphore);
        
        // 验证配置合并：limit 使用 consumer 配置，其他使用全局配置
        $this->assertEquals(5, $this->getInaccessibleProperty($semaphore, 'limit'));
        $this->assertEquals(300, $this->getInaccessibleProperty($semaphore, 'ttl'));
        $this->assertEquals(60, $this->getInaccessibleProperty($semaphore, 'acquireSleep'));
    }

    public function testBootstrapConsumerWithoutSemaphoreWhenLimitZero()
    {
        $consumerName = 'test-consumer';
        $queueName = 'test-queue';
        $callbackName = 'CallbackMock';
        $callback = $this->getMockBuilder(ConsumerInterface::class)
            ->setMockClassName($callbackName)
            ->setMethods(['execute'])
            ->getMock();
        
        $this->loadExtension([
            'id' => 'testapp',
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
                            'name' => $queueName,
                        ]
                    ],
                    'semaphore' => [
                        'type' => HashSemaphore::class,
                        'redis_component_name' => 'redis',
                        'limit' => 0, // limit <= 0，不使用信号量
                    ],
                    'consumers' => [
                        [
                            'name' => $consumerName,
                            'callbacks' => [
                                $queueName => $callbackName,
                            ],
                        ]
                    ],
                ],
            ],
        ]);
        $consumer = \Yii::$container->get(sprintf(Configuration::CONSUMER_SERVICE_NAME, $consumerName));
        $this->assertInstanceOf(Consumer::class, $consumer);
        
        // 验证 semaphore 为 null
        $semaphore = $this->getInaccessibleProperty($consumer, 'semaphore');
        $this->assertNull($semaphore);
    }

    public function testBootstrapConsumerSemaphoreConfigMerge()
    {
        $consumerName = 'test-consumer';
        $queueName = 'test-queue';
        $callbackName = 'CallbackMock';
        $callback = $this->getMockBuilder(ConsumerInterface::class)
            ->setMockClassName($callbackName)
            ->setMethods(['execute'])
            ->getMock();
        
        // 创建 Redis mock
        $redis = $this->createMock(Connection::class);
        
        $this->loadExtension([
            'id' => 'testapp',
            'components' => [
                'redis' => $redis,
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'host' => 'unreal',
                        ],
                    ],
                    'queues' => [
                        [
                            'name' => $queueName,
                        ]
                    ],
                    'semaphore' => [
                        'type' => HashSemaphore::class,
                        'redis_component_name' => 'redis',
                        'limit' => 10,
                        'ttl' => 300,
                        'acquire_sleep' => 60,
                    ],
                    'consumers' => [
                        [
                            'name' => $consumerName,
                            'callbacks' => [
                                $queueName => $callbackName,
                            ],
                            'semaphore' => [
                                'type' => IncrSemaphore::class, // Consumer 覆盖类型
                                'limit' => 5, // Consumer 覆盖 limit
                                'ttl' => 600, // Consumer 覆盖 ttl
                                // acquire_sleep 使用全局配置
                            ],
                        ]
                    ],
                ],
            ],
        ]);
        $consumer = \Yii::$container->get(sprintf(Configuration::CONSUMER_SERVICE_NAME, $consumerName));
        $semaphore = $this->getInaccessibleProperty($consumer, 'semaphore');
        
        // 验证配置合并：Consumer 配置覆盖全局配置
        $this->assertInstanceOf(IncrSemaphore::class, $semaphore);
        $this->assertEquals(5, $this->getInaccessibleProperty($semaphore, 'limit'));
        $this->assertEquals(600, $this->getInaccessibleProperty($semaphore, 'ttl'));
        $this->assertEquals(60, $this->getInaccessibleProperty($semaphore, 'acquireSleep')); // 使用全局配置
    }

    public function testBootstrapConsumerSemaphoreRedisComponentMissing()
    {
        $consumerName = 'test-consumer';
        $queueName = 'test-queue';
        $callbackName = 'CallbackMock';
        $callback = $this->getMockBuilder(ConsumerInterface::class)
            ->setMockClassName($callbackName)
            ->setMethods(['execute'])
            ->getMock();
        
        $this->loadExtension([
            'id' => 'testapp',
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
                            'name' => $queueName,
                        ]
                    ],
                    'semaphore' => [
                        'type' => HashSemaphore::class,
                        'redis_component_name' => 'redis',
                        'limit' => 10,
                    ],
                    'consumers' => [
                        [
                            'name' => $consumerName,
                            'callbacks' => [
                                $queueName => $callbackName,
                            ],
                        ]
                    ],
                ],
            ],
        ]);
        
        $this->expectException(InvalidConfigException::class);
        $this->expectExceptionMessage("Redis component 'redis' is not configured.");
        \Yii::$app->rabbitmq->getConsumer($consumerName);
    }

    /**
     * 测试多 connection 配置隔离
     * 验证每个 connection 的 routing 只包含自己的配置
     * 注意：consumer 的 name 等于它 callbacks 里的 queue 名称
     */
    public function testMultiConnectionIsolation()
    {
        $conn1Name = 'connection1';
        $conn2Name = 'connection2';
        $queue1Name = 'queue1';
        $queue2Name = 'queue2';
        $exchange1Name = 'exchange1';
        $exchange2Name = 'exchange2';
        // consumer 的 name 必须等于 queue 的 name
        $consumer1Name = $queue1Name;
        $consumer2Name = $queue2Name;
        $callbackName = 'CallbackMock';
        
        $callback = $this->getMockBuilder(ConsumerInterface::class)
            ->setMockClassName($callbackName)
            ->setMethods(['execute'])
            ->getMock();
        
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'name' => $conn1Name,
                            'host' => 'host1',
                        ],
                        [
                            'name' => $conn2Name,
                            'host' => 'host2',
                        ],
                    ],
                    'exchanges' => [
                        [
                            'name' => $exchange1Name,
                            'type' => 'direct',
                        ],
                        [
                            'name' => $exchange2Name,
                            'type' => 'direct',
                        ],
                    ],
                    'queues' => [
                        [
                            'name' => $queue1Name,
                        ],
                        [
                            'name' => $queue2Name,
                        ],
                    ],
                    'bindings' => [
                        [
                            'queue' => $queue1Name,
                            'exchange' => $exchange1Name,
                            'routing_keys' => ['routing1'],
                        ],
                        [
                            'queue' => $queue2Name,
                            'exchange' => $exchange2Name,
                            'routing_keys' => ['routing2'],
                        ],
                    ],
                    'consumers' => [
                        [
                            'name' => $consumer1Name, // 等于 queue1Name
                            'connection' => $conn1Name,
                            'callbacks' => [
                                $queue1Name => $callbackName,
                            ],
                        ],
                        [
                            'name' => $consumer2Name, // 等于 queue2Name
                            'connection' => $conn2Name,
                            'callbacks' => [
                                $queue2Name => $callbackName,
                            ],
                        ],
                    ],
                ],
            ],
        ]);
        
        // 获取两个 connection
        $conn1 = \Yii::$app->rabbitmq->getConnection($conn1Name);
        $conn2 = \Yii::$app->rabbitmq->getConnection($conn2Name);
        
        // 获取两个 routing
        $routing1 = \Yii::$app->rabbitmq->getRouting($conn1);
        $routing2 = \Yii::$app->rabbitmq->getRouting($conn2);
        
        // 验证 connection1 的 routing 只包含 queue1 和 exchange1
        $queues1 = $this->getInaccessibleProperty($routing1, 'queues');
        $exchanges1 = $this->getInaccessibleProperty($routing1, 'exchanges');
        $bindings1 = $this->getInaccessibleProperty($routing1, 'bindings');
        
        $this->assertArrayHasKey($queue1Name, $queues1, 'Connection1 should have queue1');
        $this->assertArrayNotHasKey($queue2Name, $queues1, 'Connection1 should not have queue2');
        $this->assertArrayHasKey($exchange1Name, $exchanges1, 'Connection1 should have exchange1');
        $this->assertArrayNotHasKey($exchange2Name, $exchanges1, 'Connection1 should not have exchange2');
        $this->assertCount(1, $bindings1, 'Connection1 should have 1 binding');
        $this->assertEquals($queue1Name, $bindings1[0]['queue'], 'Connection1 binding should be for queue1');
        
        // 验证 connection2 的 routing 只包含 queue2 和 exchange2
        $queues2 = $this->getInaccessibleProperty($routing2, 'queues');
        $exchanges2 = $this->getInaccessibleProperty($routing2, 'exchanges');
        $bindings2 = $this->getInaccessibleProperty($routing2, 'bindings');
        
        $this->assertArrayHasKey($queue2Name, $queues2, 'Connection2 should have queue2');
        $this->assertArrayNotHasKey($queue1Name, $queues2, 'Connection2 should not have queue1');
        $this->assertArrayHasKey($exchange2Name, $exchanges2, 'Connection2 should have exchange2');
        $this->assertArrayNotHasKey($exchange1Name, $exchanges2, 'Connection2 should not have exchange1');
        $this->assertCount(1, $bindings2, 'Connection2 should have 1 binding');
        $this->assertEquals($queue2Name, $bindings2[0]['queue'], 'Connection2 binding should be for queue2');
    }

    /**
     * 测试 Consumer name 等于 Queue name 的场景
     * 验证 getRoutingConfigByConnName 正确使用 consumer name 作为 queue name
     * 注意：consumer 的 name 等于它 callbacks 里的 queue 名称，并且只有一个 queue
     */
    public function testConsumerNameEqualsQueueName()
    {
        $connName = 'test-conn';
        $queue1Name = 'queue1';
        $queue2Name = 'queue2'; // 这个 queue 不在该 connection 的 consumer 中
        $exchange1Name = 'exchange1';
        // consumer 的 name 必须等于 queue 的 name
        $consumer1Name = $queue1Name;
        $callbackName = 'CallbackMock';
        
        $callback = $this->getMockBuilder(ConsumerInterface::class)
            ->setMockClassName($callbackName)
            ->setMethods(['execute'])
            ->getMock();
        
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'name' => $connName,
                            'host' => 'unreal',
                        ],
                    ],
                    'exchanges' => [
                        [
                            'name' => $exchange1Name,
                            'type' => 'direct',
                        ],
                    ],
                    'queues' => [
                        [
                            'name' => $queue1Name,
                        ],
                        [
                            'name' => $queue2Name,
                        ],
                    ],
                    'bindings' => [
                        [
                            'queue' => $queue1Name,
                            'exchange' => $exchange1Name,
                            'routing_keys' => ['routing1'],
                        ],
                        [
                            'queue' => $queue2Name,
                            'exchange' => $exchange1Name,
                            'routing_keys' => ['routing2'],
                        ],
                    ],
                    'consumers' => [
                        [
                            'name' => $consumer1Name, // 等于 queue1Name
                            'connection' => $connName,
                            'callbacks' => [
                                $queue1Name => $callbackName,
                            ],
                        ],
                    ],
                ],
            ],
        ]);
        
        $conn = \Yii::$app->rabbitmq->getConnection($connName);
        $routing = \Yii::$app->rabbitmq->getRouting($conn);
        
        $queues = $this->getInaccessibleProperty($routing, 'queues');
        $bindings = $this->getInaccessibleProperty($routing, 'bindings');
        
        // 验证只有 queue1 被包含（因为 consumer name = queue1 name）
        $this->assertArrayHasKey($queue1Name, $queues, 'Queue1 should be included (consumer name = queue name)');
        $this->assertArrayNotHasKey($queue2Name, $queues, 'Queue2 should not be included (not in this connection)');
        
        // 验证只有 queue1 的 binding 被包含
        $this->assertCount(1, $bindings, 'Should have 1 binding');
        $this->assertEquals($queue1Name, $bindings[0]['queue'], 'Binding should be for queue1');
    }

    /**
     * 测试 Connection name 属性
     * 验证 AbstractConnectionFactory::createConnection 正确设置 connection name
     */
    public function testConnectionNameAttribute()
    {
        $connName = 'test-connection';
        
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'name' => $connName,
                            'host' => 'unreal',
                        ],
                    ],
                ],
            ],
        ]);
        
        $conn = \Yii::$app->rabbitmq->getConnection($connName);
        
        // 验证 connection 对象有 name 属性
        $this->assertObjectHasProperty('name', $conn, 'Connection should have name attribute');
        $this->assertEquals($connName, $conn->name, 'Connection name should match');
    }

    /**
     * 测试多 connection 场景下 routing 服务名称隔离
     * 验证不同 connection 的 routing 使用不同的服务名称
     * 注意：consumer 的 name 等于它 callbacks 里的 queue 名称
     */
    public function testRoutingServiceNameIsolation()
    {
        $conn1Name = 'conn1';
        $conn2Name = 'conn2';
        $queue1Name = 'queue1';
        $queue2Name = 'queue2';
        // consumer 的 name 必须等于 queue 的 name
        $consumer1Name = $queue1Name;
        $consumer2Name = $queue2Name;
        $callbackName = 'CallbackMock';
        
        $callback = $this->getMockBuilder(ConsumerInterface::class)
            ->setMockClassName($callbackName)
            ->setMethods(['execute'])
            ->getMock();
        
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'name' => $conn1Name,
                            'host' => 'host1',
                        ],
                        [
                            'name' => $conn2Name,
                            'host' => 'host2',
                        ],
                    ],
                    'queues' => [
                        [
                            'name' => $queue1Name,
                        ],
                        [
                            'name' => $queue2Name,
                        ],
                    ],
                    'consumers' => [
                        [
                            'name' => $consumer1Name, // 等于 queue1Name
                            'connection' => $conn1Name,
                            'callbacks' => [
                                $queue1Name => $callbackName,
                            ],
                        ],
                        [
                            'name' => $consumer2Name, // 等于 queue2Name
                            'connection' => $conn2Name,
                            'callbacks' => [
                                $queue2Name => $callbackName,
                            ],
                        ],
                    ],
                ],
            ],
        ]);
        
        $conn1 = \Yii::$app->rabbitmq->getConnection($conn1Name);
        $conn2 = \Yii::$app->rabbitmq->getConnection($conn2Name);
        
        // 验证两个 routing 是不同的实例
        $routing1 = \Yii::$app->rabbitmq->getRouting($conn1);
        $routing2 = \Yii::$app->rabbitmq->getRouting($conn2);
        
        $this->assertNotSame($routing1, $routing2, 'Routings should be different instances');
        
        // 验证它们包含不同的配置
        $queues1 = $this->getInaccessibleProperty($routing1, 'queues');
        $queues2 = $this->getInaccessibleProperty($routing2, 'queues');
        
        $this->assertArrayHasKey($queue1Name, $queues1, 'Routing1 should have queue1');
        $this->assertArrayNotHasKey($queue2Name, $queues1, 'Routing1 should not have queue2');
        $this->assertArrayHasKey($queue2Name, $queues2, 'Routing2 should have queue2');
        $this->assertArrayNotHasKey($queue1Name, $queues2, 'Routing2 should not have queue1');
    }

}
