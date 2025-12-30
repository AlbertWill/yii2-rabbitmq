<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components\semaphore;

use mikemadisonweb\rabbitmq\components\semaphore\HashSemaphore;
use mikemadisonweb\rabbitmq\components\semaphore\IncrSemaphore;
use mikemadisonweb\rabbitmq\tests\TestCase;
use yii\redis\Connection;

/**
 * Semaphore 集成测试
 * 
 * 使用真实的 Redis 连接测试 semaphore 的并发场景
 * 
 * 配置说明：
 * 1. 优先从配置文件读取（推荐）：
 *    - 复制 tests/config.local.php.example 为 tests/config.local.php
 *    - 编辑 tests/config.local.php，填入实际的 Redis 连接信息
 *    - config.local.php 会被 .gitignore 忽略，不会提交到版本库
 * 
 * 2. 环境变量方式（备选）：
 *    - REDIS_HOST: Redis 主机地址（默认: localhost）
 *    - REDIS_PORT: Redis 端口（默认: 6379）
 *    - REDIS_DATABASE: Redis 数据库（默认: 0）
 *    - REDIS_PASSWORD: Redis 密码（可选）
 * 
 * 3. 如果 Redis 不可用，测试会自动跳过
 * 
 * 运行方式：
 * php vendor/bin/phpunit tests/components/semaphore/IntegrationSemaphoreTest.php
 */
class IntegrationSemaphoreTest extends TestCase
{
    /**
     * @var Connection|null Redis 连接实例
     */
    private static $redis = null;

    /**
     * 测试前检查 Redis 是否可用
     */
    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();
        
        try {
            $redis = self::createRealRedisConnection();
            $redis->open();
            // 测试连接是否可用（使用 __call 调用 PING）
            $result = $redis->__call('ping', []);
            // PING 成功返回 true 或 'PONG'
            if ($result === true || $result === 'PONG' || $result === 1) {
                self::$redis = $redis;
            } else {
                self::$redis = null;
            }
        } catch (\Exception $e) {
            // Redis 不可用，测试将跳过
            // 输出错误信息用于调试（仅在详细模式下）
            if (getenv('PHPUNIT_VERBOSE')) {
                echo "Redis connection failed: " . $e->getMessage() . "\n";
                echo "Host: " . (getenv('REDIS_HOST') ?: 'not set') . "\n";
                echo "Port: " . (getenv('REDIS_PORT') ?: 'not set') . "\n";
            }
            self::$redis = null;
        }
    }

    /**
     * 测试后清理 Redis 数据
     */
    public static function tearDownAfterClass(): void
    {
        if (self::$redis && self::$redis->getIsActive()) {
            try {
                // 清理测试使用的 key
                $testKeys = self::$redis->__call('keys', ['test:semaphore:*']);
                if (!empty($testKeys)) {
                    // 逐个删除 keys，避免 Redis Cluster 模式下的 CROSSSLOT 错误
                    foreach ($testKeys as $key) {
                        self::$redis->__call('del', [$key]);
                    }
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
     * 每个测试前检查 Redis 是否可用
     */
    protected function setUp(): void
    {
        parent::setUp();
        
        if (self::$redis === null || !self::$redis->getIsActive()) {
            $this->markTestSkipped('Redis is not available. Please configure REDIS_HOST and REDIS_PORT environment variables.');
        }
    }

    private function createIncrSemaphore(Connection $redis, string $key, int $limit, int $ttl = 600, int $acquireSleep = 60): IncrSemaphore
    {
        return new IncrSemaphore($redis, $key, $limit, $this->createSilentLogger(), $ttl, $acquireSleep);
    }

    private function createHashSemaphore(Connection $redis, string $key, int $limit, int $ttl = 600, int $acquireSleep = 60): HashSemaphore
    {
        return new HashSemaphore($redis, $key, $limit, $this->createSilentLogger(), $ttl, $acquireSleep);
    }

    /**
     * 读取测试配置文件
     * 优先从 tests/config.local.php 读取，如果不存在则返回空数组
     */
    private static function getTestConfig(): array
    {
        static $config = null;
        if ($config === null) {
            $configFile = __DIR__ . '/../../config.local.php';
            if (file_exists($configFile)) {
                $config = require $configFile;
            } else {
                $config = [];
            }
        }
        return $config;
    }

    /**
     * 创建真实的 Redis 连接
     * 优先从配置文件读取，如果没有配置文件则从环境变量读取
     */
    private static function createRealRedisConnection(): Connection
    {
        $testConfig = self::getTestConfig();
        
        // 优先从配置文件读取，如果没有则从环境变量读取，最后使用默认值
        $host = $testConfig['redis']['host'] ?? getenv('REDIS_HOST') ?: ($_ENV['REDIS_HOST'] ?? 'localhost');
        $port = (int)($testConfig['redis']['port'] ?? getenv('REDIS_PORT') ?: ($_ENV['REDIS_PORT'] ?? 6379));
        $database = (int)($testConfig['redis']['database'] ?? getenv('REDIS_DATABASE') ?: ($_ENV['REDIS_DATABASE'] ?? 0));
        $password = $testConfig['redis']['password'] ?? getenv('REDIS_PASSWORD') ?: ($_ENV['REDIS_PASSWORD'] ?? null);
        
        // 如果密码是空字符串，转换为 null
        if ($password === '') {
            $password = null;
        }
        
        // 调试输出（仅在详细模式下）
        if (getenv('PHPUNIT_VERBOSE')) {
            echo "Redis config - Host: $host, Port: $port, Database: $database, Password: " . ($password ? '***' : 'none') . "\n";
        }

        $config = [
            'hostname' => $host,
            'port' => $port,
            'database' => $database,
        ];

        if ($password !== null && $password !== '') {
            $config['password'] = $password;
        }

        return new Connection($config);
    }

    /**
     * 生成唯一的测试 key
     */
    private function generateTestKey(string $prefix = 'test:semaphore'): string
    {
        return $prefix . ':' . uniqid('', true);
    }

    /**
     * 测试 IncrSemaphore 的真实并发获取
     */
    public function testIncrSemaphoreRealConcurrentAcquire()
    {
        $limit = 5;
        $concurrentCount = 10;
        $key = $this->generateTestKey('test:incr:concurrent');
        
        $semaphores = [];
        $results = [];
        
        // 创建多个 semaphore 实例
        for ($i = 0; $i < $concurrentCount; $i++) {
            $semaphores[] = $this->createIncrSemaphore(self::$redis, $key, $limit, 600);
        }
        
        // 顺序执行 acquire（在实际场景中，这些操作会同时发生）
        // 但由于 Redis 的原子性，即使顺序执行也能验证 limit 的正确性
        for ($i = 0; $i < $concurrentCount; $i++) {
            $results[] = $semaphores[$i]->acquire();
        }
        
        // 验证成功数量不超过 limit
        $successCount = 0;
        foreach ($results as $result) {
            if ($result) {
                $successCount++;
            }
        }
        
        $this->assertEquals($limit, $successCount, "应该有 {$limit} 个成功获取");
        $this->assertLessThanOrEqual($limit, $successCount, "成功数量不应超过 limit");
        
        // 清理：释放所有已获取的 semaphore
        foreach ($semaphores as $index => $semaphore) {
            if ($results[$index]) {
                $semaphore->release();
            }
        }
    }

    /**
     * 测试 HashSemaphore 的真实并发获取
     */
    public function testHashSemaphoreRealConcurrentAcquire()
    {
        $limit = 5;
        $concurrentCount = 10;
        $key = $this->generateTestKey('test:hash:concurrent');
        
        $semaphores = [];
        $results = [];
        
        // 创建多个 semaphore 实例（每个实例有唯一的 token）
        for ($i = 0; $i < $concurrentCount; $i++) {
            $semaphores[] = $this->createHashSemaphore(self::$redis, $key, $limit, 600);
        }
        
        // 顺序执行 acquire
        for ($i = 0; $i < $concurrentCount; $i++) {
            $results[] = $semaphores[$i]->acquire();
        }
        
        // 验证成功数量不超过 limit
        $successCount = 0;
        foreach ($results as $result) {
            if ($result) {
                $successCount++;
            }
        }
        
        $this->assertEquals($limit, $successCount, "应该有 {$limit} 个成功获取");
        $this->assertLessThanOrEqual($limit, $successCount, "成功数量不应超过 limit");
        
        // 清理：释放所有已获取的 semaphore
        foreach ($semaphores as $index => $semaphore) {
            if ($results[$index]) {
                $semaphore->release();
            }
        }
    }

    /**
     * 测试获取-释放-再获取的完整流程（真实 Redis）
     */
    public function testRealAcquireReleaseAcquireFlow()
    {
        $key = $this->generateTestKey('test:acquire:release');
        $limit = 3;
        
        // 第一轮：获取
        $semaphore1 = $this->createIncrSemaphore(self::$redis, $key, $limit, 600);
        $result1 = $semaphore1->acquire();
        $this->assertTrue($result1, '第一次获取应该成功');
        
        // 释放
        $semaphore1->release();
        
        // 第二轮：再次获取
        $semaphore2 = $this->createIncrSemaphore(self::$redis, $key, $limit, 600);
        $result2 = $semaphore2->acquire();
        $this->assertTrue($result2, '释放后应该可以再次获取');
        
        // 清理
        $semaphore2->release();
    }

    /**
     * 测试达到 limit 后释放，其他实例可以获取（真实 Redis）
     */
    public function testRealAcquireUntilLimitThenReleaseAndAcquire()
    {
        $key = $this->generateTestKey('test:limit:release');
        $limit = 3;
        
        $semaphores = [];
        $results = [];
        
        // 创建多个实例
        for ($i = 0; $i < $limit + 2; $i++) {
            $semaphores[] = $this->createHashSemaphore(self::$redis, $key, $limit, 600);
        }
        
        // 连续获取直到达到 limit
        for ($i = 0; $i < $limit; $i++) {
            $results[] = $semaphores[$i]->acquire();
            $this->assertTrue($results[$i], "第 " . ($i + 1) . " 次获取应该成功");
        }
        
        // 达到 limit 后应该无法获取
        $result = $semaphores[$limit]->acquire();
        $this->assertFalse($result, '达到 limit 后应该无法获取');
        
        // 释放第一个
        $semaphores[0]->release();
        
        // 释放后应该可以再次获取
        $result = $semaphores[$limit]->acquire();
        $this->assertTrue($result, '释放后应该可以再次获取');
        
        // 清理：释放所有已获取的
        for ($i = 1; $i < $limit; $i++) {
            $semaphores[$i]->release();
        }
        $semaphores[$limit]->release();
    }

    /**
     * 测试多次获取和释放的循环（真实 Redis）
     */
    public function testRealMultipleAcquireReleaseCycles()
    {
        $key = $this->generateTestKey('test:cycles');
        $limit = 5;
        $cycles = 10;
        
        $semaphore = $this->createIncrSemaphore(self::$redis, $key, $limit, 600);
        
        // 执行多次获取-释放循环
        for ($i = 0; $i < $cycles; $i++) {
            $result = $semaphore->acquire();
            $this->assertTrue($result, "第 " . ($i + 1) . " 次循环获取应该成功");
            
            $semaphore->release();
        }
    }

    /**
     * 测试 heartbeat 续期功能（真实 Redis）
     */
    public function testRealHeartbeat()
    {
        $key = $this->generateTestKey('test:heartbeat');
        
        $semaphore = $this->createIncrSemaphore(self::$redis, $key, 5, 10); // TTL = 10 秒
        
        // 获取 semaphore
        $result = $semaphore->acquire();
        $this->assertTrue($result, '获取应该成功');
        
        // 等待 3 秒，让 TTL 减少
        sleep(3);
        
        // 检查 TTL（应该小于 10）
        $ttl1 = self::$redis->__call('ttl', [$key]);
        $this->assertGreaterThan(0, $ttl1, 'TTL 应该大于 0');
        $this->assertLessThan(10, $ttl1, '等待后 TTL 应该小于 10');
        
        // 执行 heartbeat
        $semaphore->heartbeat();
        
        // 检查 TTL 是否被刷新（应该接近 10）
        $ttl2 = self::$redis->__call('ttl', [$key]);
        $this->assertGreaterThan($ttl1, $ttl2, 'heartbeat 后 TTL 应该被刷新');
        $this->assertLessThanOrEqual(10, $ttl2, 'TTL 应该小于等于 10');
        $this->assertGreaterThan(8, $ttl2, 'TTL 应该接近 10（至少大于 8）');
        
        // 清理
        $semaphore->release();
    }

    /**
     * 测试 acquire_wait 功能（真实 Redis）
     * 
     * 注意：由于 PHP 是单线程的，无法真正模拟异步释放场景
     * 此测试主要验证 acquire_wait 在信号量可用时能正确获取
     */
    public function testRealAcquireWait()
    {
        $key = $this->generateTestKey('test:acquire:wait');
        $limit = 1;
        
        // 第一个实例获取成功
        $semaphore1 = $this->createIncrSemaphore(self::$redis, $key, $limit, 600, 1); // acquireSleep = 1 秒
        $result1 = $semaphore1->acquire();
        $this->assertTrue($result1, '第一个实例应该成功获取');
        
        // 第二个实例应该无法立即获取（因为 limit = 1）
        $semaphore2 = $this->createIncrSemaphore(self::$redis, $key, $limit, 600, 1);
        $result2 = $semaphore2->acquire();
        $this->assertFalse($result2, '第二个实例应该无法立即获取');
        
        // 释放第一个实例
        $semaphore1->release();
        
        // 第二个实例使用 acquire_wait 应该可以获取
        // 由于信号量已经可用，acquire_wait 会立即成功（不会等待）
        $result3 = $semaphore2->acquire_wait();
        $this->assertTrue($result3, '释放后，acquire_wait 应该可以获取');
        
        // 验证 acquire_wait 的基本功能：能够成功获取已释放的信号量
        // 注意：在单线程环境中无法测试真正的等待行为，因为无法在 acquire_wait 执行过程中异步释放
        
        // 清理
        $semaphore2->release();
    }

    /**
     * 测试边界情况：limit = 1（真实 Redis）
     */
    public function testRealAcquireReleaseWithLimitOne()
    {
        $key = $this->generateTestKey('test:limit:one');
        
        $semaphore1 = $this->createHashSemaphore(self::$redis, $key, 1, 600);
        $semaphore2 = $this->createHashSemaphore(self::$redis, $key, 1, 600);
        $semaphore3 = $this->createHashSemaphore(self::$redis, $key, 1, 600);
        
        // 第一次获取应该成功
        $result1 = $semaphore1->acquire();
        $this->assertTrue($result1, 'limit=1 时第一次获取应该成功');
        
        // 第二次获取应该失败
        $result2 = $semaphore2->acquire();
        $this->assertFalse($result2, 'limit=1 时第二次获取应该失败');
        
        // 释放第一个
        $semaphore1->release();
        
        // 释放后应该可以再次获取
        $result3 = $semaphore3->acquire();
        $this->assertTrue($result3, '释放后应该可以再次获取');
        
        // 清理
        $semaphore3->release();
    }

    /**
     * 测试并发场景：多个实例同时获取，验证实际成功数量（真实 Redis）
     * 
     * 这个测试模拟真实的并发场景，通过快速顺序执行来模拟并发
     */
    public function testRealConcurrentAcquireWithStatistics()
    {
        $key = $this->generateTestKey('test:concurrent:stats');
        $limit = 5;
        $concurrentCount = 20; // 20 个并发请求
        
        $semaphores = [];
        $results = [];
        $timings = [];
        
        // 创建多个 semaphore 实例
        for ($i = 0; $i < $concurrentCount; $i++) {
            $semaphores[] = $this->createHashSemaphore(self::$redis, $key, $limit, 600);
        }
        
        // 快速顺序执行 acquire（模拟并发）
        $startTime = microtime(true);
        for ($i = 0; $i < $concurrentCount; $i++) {
            $acquireStart = microtime(true);
            $results[] = $semaphores[$i]->acquire();
            $acquireEnd = microtime(true);
            $timings[] = $acquireEnd - $acquireStart;
        }
        $endTime = microtime(true);
        
        // 统计结果
        $successCount = 0;
        $failCount = 0;
        foreach ($results as $result) {
            if ($result) {
                $successCount++;
            } else {
                $failCount++;
            }
        }
        
        // 验证结果
        $this->assertEquals($limit, $successCount, "应该有 {$limit} 个成功获取");
        $this->assertEquals($concurrentCount - $limit, $failCount, "应该有 " . ($concurrentCount - $limit) . " 个失败");
        $this->assertLessThan(5.0, $endTime - $startTime, "总执行时间应该小于 5 秒");
        
        // 输出统计信息（用于调试）
        $avgTime = array_sum($timings) / count($timings);
        $maxTime = max($timings);
        $minTime = min($timings);
        
        echo "\n并发测试统计:\n";
        echo "  总请求数: {$concurrentCount}\n";
        echo "  成功数: {$successCount}\n";
        echo "  失败数: {$failCount}\n";
        echo "  总耗时: " . round($endTime - $startTime, 3) . " 秒\n";
        echo "  平均耗时: " . round($avgTime * 1000, 2) . " 毫秒\n";
        echo "  最大耗时: " . round($maxTime * 1000, 2) . " 毫秒\n";
        echo "  最小耗时: " . round($minTime * 1000, 2) . " 毫秒\n";
        
        // 清理：释放所有已获取的 semaphore
        foreach ($semaphores as $index => $semaphore) {
            if ($results[$index]) {
                $semaphore->release();
            }
        }
    }
}

