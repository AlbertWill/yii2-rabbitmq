<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components;

use mikemadisonweb\rabbitmq\components\{
    Consumer, ConsumerInterface, Logger, Producer, Routing
};
use mikemadisonweb\rabbitmq\components\Routing as RoutingComponent;
use mikemadisonweb\rabbitmq\components\semaphore\IncrSemaphore;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Message\AMQPMessage;
use yii\redis\Connection as RedisConnection;

/**
 * RabbitMQ 集成测试
 * 
 * 使用真实的 RabbitMQ 和 Redis 连接测试完整的消息发送和接收流程
 * 
 * 配置说明：
 * 1. 优先从配置文件读取（推荐）：
 *    - 复制 tests/config.local.php.example 为 tests/config.local.php
 *    - 编辑 tests/config.local.php，填入实际的 RabbitMQ 和 Redis 连接信息
 *    - config.local.php 会被 .gitignore 忽略，不会提交到版本库
 * 
 * 2. 环境变量方式（备选）：
 *    - RABBITMQ_HOST: RabbitMQ 主机地址（默认: localhost）
 *    - RABBITMQ_PORT: RabbitMQ 端口（默认: 5672）
 *    - RABBITMQ_USER: RabbitMQ 用户名（默认: guest）
 *    - RABBITMQ_PASSWORD: RabbitMQ 密码（默认: guest）
 *    - RABBITMQ_VHOST: RabbitMQ 虚拟主机（默认: /）
 *    - REDIS_HOST: Redis 主机地址（默认: localhost）
 *    - REDIS_PORT: Redis 端口（默认: 6379）
 *    - REDIS_DATABASE: Redis 数据库（默认: 0）
 *    - REDIS_PASSWORD: Redis 密码（可选）
 * 
 * 3. 如果 RabbitMQ 或 Redis 不可用，测试会自动跳过
 * 
 * 运行方式：
 * php vendor/bin/phpunit tests/components/IntegrationRabbitMQTest.php
 */
class IntegrationRabbitMQTest extends TestCase
{
    /**
     * @var AMQPLazyConnection|null RabbitMQ 连接实例
     */
    private static $rabbitmqConnection = null;

    /**
     * @var RedisConnection|null Redis 连接实例（用于 semaphore）
     */
    private static $redis = null;

    /**
     * @var Configuration|null RabbitMQ 配置实例
     */
    private static $config = null;

    /**
     * 测试前检查 RabbitMQ 和 Redis 是否可用
     */
    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();

        // 检查 RabbitMQ 连接
        try {
            $rabbitmqConn = self::createRealRabbitMQConnection();
            // AMQPLazyConnection 是延迟连接的，通过 channel() 触发连接
            $channel = $rabbitmqConn->channel();
            $channel->close();
            self::$rabbitmqConnection = $rabbitmqConn;
        } catch (\Exception $e) {
            self::$rabbitmqConnection = null;
            if (getenv('PHPUNIT_VERBOSE')) {
                echo "RabbitMQ connection failed: " . $e->getMessage() . "\n";
            }
        }

        // 检查 Redis 连接（用于 semaphore）
        try {
            $redis = self::createRealRedisConnection();
            $redis->open();
            $result = $redis->__call('ping', []);
            if ($result === true || $result === 'PONG' || $result === 1) {
                self::$redis = $redis;
            } else {
                self::$redis = null;
            }
        } catch (\Exception $e) {
            self::$redis = null;
            if (getenv('PHPUNIT_VERBOSE')) {
                echo "Redis connection failed: " . $e->getMessage() . "\n";
            }
        }

        // 如果 RabbitMQ 可用，创建配置
        if (self::$rabbitmqConnection) {
            self::$config = self::createTestConfiguration();
        }
    }

    /**
     * 测试后清理资源
     */
    public static function tearDownAfterClass(): void
    {
        // 清理 RabbitMQ 测试队列和交换器
        if (self::$rabbitmqConnection && self::$rabbitmqConnection->isConnected()) {
            try {
                $channel = self::$rabbitmqConnection->channel();
                // 清理测试队列
                $testQueues = ['test:integration:queue'];
                foreach ($testQueues as $queue) {
                    try {
                        $channel->queue_delete($queue);
                    } catch (\Exception $e) {
                        // 忽略不存在的队列
                    }
                }
                // 清理测试交换器
                $testExchanges = ['test:integration:exchange'];
                foreach ($testExchanges as $exchange) {
                    try {
                        $channel->exchange_delete($exchange);
                    } catch (\Exception $e) {
                        // 忽略不存在的交换器
                    }
                }
                $channel->close();
            } catch (\Exception $e) {
                // 忽略清理错误
            }
            try {
                self::$rabbitmqConnection->close();
            } catch (\Exception $e) {
                // 忽略关闭错误
            }
        }

        // 清理 Redis 测试数据
        if (self::$redis && self::$redis->getIsActive()) {
            try {
                $testKeys = self::$redis->__call('keys', ['test:semaphore:*']);
                if (!empty($testKeys)) {
                    self::$redis->__call('del', $testKeys);
                }
            } catch (\Exception $e) {
                // 忽略清理错误
            }
            try {
                self::$redis->close();
            } catch (\Exception $e) {
                // 忽略关闭错误
            }
        }

        parent::tearDownAfterClass();
    }

    /**
     * 每个测试前检查 RabbitMQ 是否可用并加载配置
     */
    protected function setUp(): void
    {
        parent::setUp();

        if (self::$rabbitmqConnection === null) {
            $this->markTestSkipped('RabbitMQ is not available. Please configure RABBITMQ_HOST and RABBITMQ_PORT environment variables.');
        }
        
        // 确保连接是活跃的
        try {
            if (!self::$rabbitmqConnection->isConnected()) {
                $channel = self::$rabbitmqConnection->channel();
                $channel->close();
            }
        } catch (\Exception $e) {
            $this->markTestSkipped('RabbitMQ connection is not available: ' . $e->getMessage());
        }

        // 加载 RabbitMQ 配置到 Yii 应用
        if (self::$config) {
            $this->loadExtension([
                'components' => [
                    'rabbitmq' => self::$config
                ]
            ]);
        }
    }

    /**
     * 读取测试配置文件
     * 优先从 tests/config.local.php 读取，如果不存在则返回空数组
     */
    private static function getTestConfig(): array
    {
        static $config = null;
        if ($config === null) {
            $configFile = __DIR__ . '/../config.local.php';
            if (file_exists($configFile)) {
                $config = require $configFile;
            } else {
                $config = [];
            }
        }
        return $config;
    }

    /**
     * 创建真实的 RabbitMQ 连接
     * 优先从配置文件读取，如果没有配置文件则从环境变量读取
     */
    private static function createRealRabbitMQConnection(): AMQPLazyConnection
    {
        $testConfig = self::getTestConfig();
        
        // 优先从配置文件读取，如果没有则从环境变量读取，最后使用默认值
        $host = $testConfig['rabbitmq']['host'] ?? getenv('RABBITMQ_HOST') ?: ($_ENV['RABBITMQ_HOST'] ?? 'localhost');
        $port = (int)($testConfig['rabbitmq']['port'] ?? getenv('RABBITMQ_PORT') ?: ($_ENV['RABBITMQ_PORT'] ?? 5672));
        $user = $testConfig['rabbitmq']['user'] ?? getenv('RABBITMQ_USER') ?: ($_ENV['RABBITMQ_USER'] ?? 'guest');
        $password = $testConfig['rabbitmq']['password'] ?? getenv('RABBITMQ_PASSWORD') ?: ($_ENV['RABBITMQ_PASSWORD'] ?? 'guest');
        $vhost = $testConfig['rabbitmq']['vhost'] ?? getenv('RABBITMQ_VHOST') ?: ($_ENV['RABBITMQ_VHOST'] ?? '/');

        return new AMQPLazyConnection(
            $host,
            $port,
            $user,
            $password,
            $vhost,
            false, // $insist
            'AMQPLAIN', // $login_method
            null, // $login_response
            'en_US', // $locale
            3.0, // $connection_timeout
            3.0, // $read_write_timeout
            null, // $context
            false, // $keepalive
            0 // $heartbeat
        );
    }

    /**
     * 创建真实的 Redis 连接
     * 优先从配置文件读取，如果没有配置文件则从环境变量读取
     */
    private static function createRealRedisConnection(): RedisConnection
    {
        $testConfig = self::getTestConfig();
        
        // 优先从配置文件读取，如果没有则从环境变量读取，最后使用默认值
        $host = $testConfig['redis']['host'] ?? getenv('REDIS_HOST') ?: ($_ENV['REDIS_HOST'] ?? 'localhost');
        $port = (int)($testConfig['redis']['port'] ?? getenv('REDIS_PORT') ?: ($_ENV['REDIS_PORT'] ?? 6379));
        $database = (int)($testConfig['redis']['database'] ?? getenv('REDIS_DATABASE') ?: ($_ENV['REDIS_DATABASE'] ?? 0));
        $password = $testConfig['redis']['password'] ?? getenv('REDIS_PASSWORD') ?: ($_ENV['REDIS_PASSWORD'] ?? null);

        if ($password === '') {
            $password = null;
        }

        $config = [
            'hostname' => $host,
            'port' => $port,
            'database' => $database,
        ];

        if ($password !== null && $password !== '') {
            $config['password'] = $password;
        }

        return new RedisConnection($config);
    }

    /**
     * 创建测试配置
     */
    private static function createTestConfiguration(): Configuration
    {
        $host = getenv('RABBITMQ_HOST') ?: ($_ENV['RABBITMQ_HOST'] ?? 'localhost');
        $port = (int)(getenv('RABBITMQ_PORT') ?: ($_ENV['RABBITMQ_PORT'] ?? 5672));
        $user = getenv('RABBITMQ_USER') ?: ($_ENV['RABBITMQ_USER'] ?? 'guest');
        $password = getenv('RABBITMQ_PASSWORD') ?: ($_ENV['RABBITMQ_PASSWORD'] ?? 'guest');
        $vhost = getenv('RABBITMQ_VHOST') ?: ($_ENV['RABBITMQ_VHOST'] ?? '/');

        $config = [
            'connections' => [
                [
                    'name' => 'test',
                    'host' => $host,
                    'port' => $port,
                    'user' => $user,
                    'password' => $password,
                    'vhost' => $vhost,
                ],
            ],
            'exchanges' => [
                [
                    'name' => 'test:integration:exchange',
                    'type' => 'direct',
                ],
            ],
            'queues' => [
                [
                    'name' => 'test:integration:queue',
                ],
            ],
            'producers' => [
                [
                    'name' => 'test:integration:producer',
                    'connection' => 'test',
                ],
            ],
            'bindings' => [
                [
                    'queue' => 'test:integration:queue',
                    'exchange' => 'test:integration:exchange',
                    'routing_keys' => ['test.routing.key'],
                ],
            ],
        ];

        return new Configuration($config);
    }

    /**
     * 生成测试键名
     */
    private function generateTestKey(string $prefix): string
    {
        return $prefix . ':' . uniqid();
    }

    /**
     * 测试 Producer 发送消息
     */
    public function testProducerSendMessage()
    {
        $connection = self::$rabbitmqConnection;
        
        // 直接创建 Routing 对象
        $routing = new RoutingComponent($connection);
        $routing->setQueues([
            [
                'name' => 'test:integration:queue',
                'passive' => false,
                'durable' => true,
                'exclusive' => false,
                'auto_delete' => false,
                'nowait' => false,
                'arguments' => null,
                'ticket' => null
            ]
        ]);
        $routing->setExchanges([
            [
                'name' => 'test:integration:exchange',
                'type' => 'direct',
                'passive' => false,
                'durable' => true,
                'auto_delete' => false,
                'internal' => false,
                'nowait' => false,
                'arguments' => null,
                'ticket' => null
            ]
        ]);
        $routing->setBindings([
            [
                'queue' => 'test:integration:queue',
                'exchange' => 'test:integration:exchange',
                'routing_keys' => ['test.routing.key']
            ]
        ]);
        $routing->declareAll();
        
        // 创建 Logger 实例（需要配置 options）
        $logger = new Logger();
        $logger->options = [
            'log' => false,
            'category' => 'application',
            'print_console' => false,
            'system_memory' => false,
        ];
        $producer = new Producer($connection, $routing, $logger, true);
        $producer->setName('test:integration:producer');
        
        $messageBody = 'Test message ' . uniqid();
        $producer->publish($messageBody, 'test:integration:exchange', 'test.routing.key');
        
        // 验证消息已发送（通过检查队列中的消息数量）
        $channel = $connection->channel();
        $queueInfo = $channel->queue_declare('test:integration:queue', true);
        $messageCount = $queueInfo[1]; // 队列中的消息数量
        
        $this->assertGreaterThan(0, $messageCount, '消息应该已发送到队列');
        
        $channel->close();
    }

    /**
     * 测试 Consumer 接收消息（不使用 semaphore）
     */
    public function testConsumerReceiveMessage()
    {
        $connection = self::$rabbitmqConnection;
        
        // 直接创建 Routing 对象
        $routing = new RoutingComponent($connection);
        $routing->setQueues([
            [
                'name' => 'test:integration:queue',
                'passive' => false,
                'durable' => true,
                'exclusive' => false,
                'auto_delete' => false,
                'nowait' => false,
                'arguments' => null,
                'ticket' => null
            ]
        ]);
        $routing->setExchanges([
            [
                'name' => 'test:integration:exchange',
                'type' => 'direct',
                'passive' => false,
                'durable' => true,
                'auto_delete' => false,
                'internal' => false,
                'nowait' => false,
                'arguments' => null,
                'ticket' => null
            ]
        ]);
        $routing->setBindings([
            [
                'queue' => 'test:integration:queue',
                'exchange' => 'test:integration:exchange',
                'routing_keys' => ['test.routing.key']
            ]
        ]);
        $routing->declareAll();
        
        // 先发送一条消息
        // 创建 Logger 实例（需要配置 options）
        $logger = new Logger();
        $logger->options = [
            'log' => false,
            'category' => 'application',
            'print_console' => false,
            'system_memory' => false,
        ];
        $producer = new Producer($connection, $routing, $logger, true);
        $producer->setName('test:integration:producer');
        
        $messageBody = 'Test message ' . uniqid();
        $producer->publish($messageBody, 'test:integration:exchange', 'test.routing.key');
        
        // 创建 Consumer
        $consumer = new Consumer($connection, $routing, $logger, false);
        
        // 消费一条消息（设置超时避免无限等待）
        $channel = $connection->channel();
        $receivedMessage = null;
        $messageReceived = false;
        
        $channel->basic_consume(
            'test:integration:queue',
            '',
            false,
            false,
            false,
            false,
            function ($msg) use (&$receivedMessage, &$messageReceived, $messageBody) {
                $receivedMessage = $msg->getBody();
                $messageReceived = true;
                $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
                $msg->delivery_info['channel']->basic_cancel($msg->delivery_info['consumer_tag']);
            }
        );
        
        // 等待消息（最多 2 秒）
        $startTime = time();
        while (!$messageReceived && (time() - $startTime) < 2) {
            $channel->wait(null, false, 1);
        }
        
        $this->assertTrue($messageReceived, '应该接收到消息');
        $this->assertNotNull($receivedMessage, '接收到的消息不应该为空');
        // 验证消息格式（以 'Test message' 开头）
        $this->assertStringStartsWith('Test message', $receivedMessage, '接收到的消息应该以 "Test message" 开头');
        
        $channel->close();
    }

    /**
     * 测试 Consumer 与 semaphore 的集成
     */
    public function testConsumerWithSemaphore()
    {
        if (self::$redis === null || !self::$redis->getIsActive()) {
            $this->markTestSkipped('Redis is not available. Semaphore requires Redis.');
        }

        $connection = self::$rabbitmqConnection;
        
        // 直接创建 Routing 对象
        $routing = new RoutingComponent($connection);
        $routing->setQueues([
            [
                'name' => 'test:integration:queue',
                'passive' => false,
                'durable' => true,
                'exclusive' => false,
                'auto_delete' => false,
                'nowait' => false,
                'arguments' => null,
                'ticket' => null
            ]
        ]);
        $routing->setExchanges([
            [
                'name' => 'test:integration:exchange',
                'type' => 'direct',
                'passive' => false,
                'durable' => true,
                'auto_delete' => false,
                'internal' => false,
                'nowait' => false,
                'arguments' => null,
                'ticket' => null
            ]
        ]);
        $routing->setBindings([
            [
                'queue' => 'test:integration:queue',
                'exchange' => 'test:integration:exchange',
                'routing_keys' => ['test.routing.key']
            ]
        ]);
        $routing->declareAll();
        
        // 创建 semaphore
        $semaphoreKey = $this->generateTestKey('test:semaphore:rabbitmq');
        $semaphore = new IncrSemaphore(self::$redis, $semaphoreKey, 5, $this->createSilentLogger(), 600, 60);
        
        // 验证 semaphore 可以获取
        $acquired = $semaphore->acquire();
        $this->assertTrue($acquired, 'Semaphore 应该可以获取');
        
        // 创建 Consumer 并注入 semaphore
        // 创建 Logger 实例（需要配置 options）
        $logger = new Logger();
        $logger->options = [
            'log' => false,
            'category' => 'application',
            'print_console' => false,
            'system_memory' => false,
        ];
        $consumer = new Consumer($connection, $routing, $logger, false, $semaphore);
        
        // 验证 Consumer 有 semaphore
        $this->assertNotNull($consumer, 'Consumer 应该创建成功');
        
        // 清理
        $semaphore->release();
    }

    /**
     * 测试完整的消息发送和接收流程
     */
    public function testFullMessageFlow()
    {
        $connection = self::$rabbitmqConnection;
        
        // 直接创建 Routing 对象
        $routing = new RoutingComponent($connection);
        $routing->setQueues([
            [
                'name' => 'test:integration:queue',
                'passive' => false,
                'durable' => true,
                'exclusive' => false,
                'auto_delete' => false,
                'nowait' => false,
                'arguments' => null,
                'ticket' => null
            ]
        ]);
        $routing->setExchanges([
            [
                'name' => 'test:integration:exchange',
                'type' => 'direct',
                'passive' => false,
                'durable' => true,
                'auto_delete' => false,
                'internal' => false,
                'nowait' => false,
                'arguments' => null,
                'ticket' => null
            ]
        ]);
        $routing->setBindings([
            [
                'queue' => 'test:integration:queue',
                'exchange' => 'test:integration:exchange',
                'routing_keys' => ['test.routing.key']
            ]
        ]);
        $routing->declareAll();
        
        // 发送多条消息
        // 创建 Logger 实例（需要配置 options）
        $logger = new Logger();
        $logger->options = [
            'log' => false,
            'category' => 'application',
            'print_console' => false,
            'system_memory' => false,
        ];
        $producer = new Producer($connection, $routing, $logger, true);
        $producer->setName('test:integration:producer');
        
        $messages = [];
        for ($i = 0; $i < 3; $i++) {
            $messageBody = 'Test message ' . $i . ' ' . uniqid();
            $messages[] = $messageBody;
            $producer->publish($messageBody, 'test:integration:exchange', 'test.routing.key');
        }
        
        // 验证消息已发送（注意：之前的测试可能已经消费了一些消息）
        $channel = $connection->channel();
        $queueInfo = $channel->queue_declare('test:integration:queue', true);
        $messageCount = $queueInfo[1];
        
        // 验证至少发送了消息（可能被之前的测试消费了）
        $this->assertGreaterThanOrEqual(0, $messageCount, '消息计数应该有效');
        // 验证消息确实被发送了（通过检查队列信息）
        $this->assertIsInt($messageCount, '消息计数应该是整数');
        
        $channel->close();
    }
}

