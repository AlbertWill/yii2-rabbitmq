<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components\semaphore;

use mikemadisonweb\rabbitmq\components\semaphore\HashSemaphore;
use mikemadisonweb\rabbitmq\components\semaphore\IncrSemaphore;
use mikemadisonweb\rabbitmq\tests\TestCase;
use yii\redis\Connection;

/**
 * Semaphore 性能测试
 * 
 * 使用真实的 Redis 连接测试 semaphore 的性能指标
 * 
 * 配置说明：
 * 1. 通过环境变量配置 Redis 连接信息：
 *    - REDIS_HOST: Redis 主机地址（默认: localhost）
 *    - REDIS_PORT: Redis 端口（默认: 6379）
 *    - REDIS_DATABASE: Redis 数据库（默认: 0）
 *    - REDIS_PASSWORD: Redis 密码（可选）
 * 
 * 2. 如果 Redis 不可用，测试会自动跳过
 * 
 * 运行方式：
 * REDIS_HOST=localhost REDIS_PORT=6379 php phpunit.phar tests/components/semaphore/PerformanceSemaphoreTest.php
 */
class PerformanceSemaphoreTest extends TestCase
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
            $result = $redis->__call('ping', []);
            if ($result === true || $result === 'PONG' || $result === 1) {
                self::$redis = $redis;
            } else {
                self::$redis = null;
            }
        } catch (\Exception $e) {
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
                $testKeys = self::$redis->__call('keys', ['test:semaphore:perf:*']);
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
     * 每个测试前检查 Redis 是否可用
     */
    protected function setUp(): void
    {
        parent::setUp();
        
        if (self::$redis === null || !self::$redis->getIsActive()) {
            $this->markTestSkipped('Redis is not available. Please configure REDIS_HOST and REDIS_PORT environment variables.');
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

        return new Connection($config);
    }

    /**
     * 生成测试键名
     */
    private function generateTestKey(string $prefix): string
    {
        return $prefix . ':' . uniqid();
    }

    /**
     * 测试 IncrSemaphore acquire 性能
     */
    public function testIncrSemaphoreAcquirePerformance()
    {
        $key = $this->generateTestKey('test:semaphore:perf:incr:acquire');
        $limit = 100;
        $iterations = 1000;
        
        $semaphore = new IncrSemaphore(self::$redis, $key, $limit, 600);
        
        $startTime = microtime(true);
        $successCount = 0;
        
        for ($i = 0; $i < $iterations; $i++) {
            if ($semaphore->acquire()) {
                $successCount++;
            }
        }
        
        $endTime = microtime(true);
        $totalTime = $endTime - $startTime;
        $avgTime = ($totalTime / $iterations) * 1000; // 转换为毫秒
        
        // 清理
        for ($i = 0; $i < $successCount; $i++) {
            $semaphore->release();
        }
        
        echo "\n=== IncrSemaphore Acquire 性能测试 ===\n";
        echo "总操作数: {$iterations}\n";
        echo "成功数: {$successCount}\n";
        echo "总耗时: " . number_format($totalTime, 4) . " 秒\n";
        echo "平均耗时: " . number_format($avgTime, 2) . " 毫秒/次\n";
        echo "吞吐量: " . number_format($iterations / $totalTime, 2) . " 操作/秒\n";
        
        // 性能断言：平均耗时应该小于 1 毫秒（提高性能基准）
        // 注意：实际性能取决于网络延迟和 Redis 服务器性能
        $this->assertLessThan(1.0, $avgTime, '平均耗时应该小于 1 毫秒');
    }

    /**
     * 测试 HashSemaphore acquire 性能
     */
    public function testHashSemaphoreAcquirePerformance()
    {
        $key = $this->generateTestKey('test:semaphore:perf:hash:acquire');
        $limit = 100;
        $iterations = 1000;
        
        $semaphores = [];
        $startTime = microtime(true);
        $successCount = 0;
        
        for ($i = 0; $i < $iterations; $i++) {
            $semaphore = new HashSemaphore(self::$redis, $key, $limit, 600);
            if ($semaphore->acquire()) {
                $successCount++;
                $semaphores[] = $semaphore;
            }
        }
        
        $endTime = microtime(true);
        $totalTime = $endTime - $startTime;
        $avgTime = ($totalTime / $iterations) * 1000; // 转换为毫秒
        
        // 清理
        foreach ($semaphores as $semaphore) {
            $semaphore->release();
        }
        
        echo "\n=== HashSemaphore Acquire 性能测试 ===\n";
        echo "总操作数: {$iterations}\n";
        echo "成功数: {$successCount}\n";
        echo "总耗时: " . number_format($totalTime, 4) . " 秒\n";
        echo "平均耗时: " . number_format($avgTime, 2) . " 毫秒/次\n";
        echo "吞吐量: " . number_format($iterations / $totalTime, 2) . " 操作/秒\n";
        
        // 性能断言：平均耗时应该小于 2 毫秒（HashSemaphore 需要生成 token，稍慢，提高性能基准）
        // 注意：实际性能取决于网络延迟和 Redis 服务器性能
        $this->assertLessThan(2.0, $avgTime, '平均耗时应该小于 2 毫秒');
    }

    /**
     * 测试 acquire-release 循环性能
     */
    public function testAcquireReleaseCyclePerformance()
    {
        $key = $this->generateTestKey('test:semaphore:perf:cycle');
        $limit = 10;
        $cycles = 500;
        
        $semaphore = new IncrSemaphore(self::$redis, $key, $limit, 600);
        
        $startTime = microtime(true);
        
        for ($i = 0; $i < $cycles; $i++) {
            $semaphore->acquire();
            $semaphore->release();
        }
        
        $endTime = microtime(true);
        $totalTime = $endTime - $startTime;
        $avgTime = ($totalTime / $cycles) * 1000; // 转换为毫秒
        
        echo "\n=== Acquire-Release 循环性能测试 ===\n";
        echo "循环次数: {$cycles}\n";
        echo "总耗时: " . number_format($totalTime, 4) . " 秒\n";
        echo "平均耗时: " . number_format($avgTime, 2) . " 毫秒/循环\n";
        echo "吞吐量: " . number_format($cycles / $totalTime, 2) . " 循环/秒\n";
        
        // 性能断言：平均耗时应该小于 2 毫秒（acquire + release 两次操作，提高性能基准）
        // 注意：实际性能取决于网络延迟和 Redis 服务器性能
        $this->assertLessThan(2.0, $avgTime, '平均耗时应该小于 2 毫秒');
    }

    /**
     * 测试高并发 acquire 性能（100+ 并发）
     */
    public function testHighConcurrencyAcquirePerformance()
    {
        $key = $this->generateTestKey('test:semaphore:perf:high:concurrency');
        $limit = 50;
        $concurrentCount = 200; // 200 个并发请求
        
        $startTime = microtime(true);
        $results = [];
        
        // 创建多个 semaphore 实例模拟并发
        for ($i = 0; $i < $concurrentCount; $i++) {
            $semaphore = new IncrSemaphore(self::$redis, $key, $limit, 600);
            $acquireStart = microtime(true);
            $result = $semaphore->acquire();
            $acquireEnd = microtime(true);
            
            $results[] = [
                'success' => $result,
                'time' => ($acquireEnd - $acquireStart) * 1000, // 毫秒
                'semaphore' => $semaphore,
            ];
        }
        
        $endTime = microtime(true);
        $totalTime = $endTime - $startTime;
        
        // 统计
        $successCount = 0;
        $failureCount = 0;
        $times = [];
        foreach ($results as $result) {
            if ($result['success']) {
                $successCount++;
            } else {
                $failureCount++;
            }
            $times[] = $result['time'];
        }
        
        $avgTime = array_sum($times) / count($times);
        $minTime = min($times);
        $maxTime = max($times);
        $p95Time = $this->percentile($times, 95);
        $p99Time = $this->percentile($times, 99);
        
        // 清理
        foreach ($results as $result) {
            if ($result['success']) {
                $result['semaphore']->release();
            }
        }
        
        echo "\n=== 高并发 Acquire 性能测试 ===\n";
        echo "并发数: {$concurrentCount}\n";
        echo "Limit: {$limit}\n";
        echo "成功数: {$successCount}\n";
        echo "失败数: {$failureCount}\n";
        echo "总耗时: " . number_format($totalTime, 4) . " 秒\n";
        echo "平均耗时: " . number_format($avgTime, 2) . " 毫秒\n";
        echo "最小耗时: " . number_format($minTime, 2) . " 毫秒\n";
        echo "最大耗时: " . number_format($maxTime, 2) . " 毫秒\n";
        echo "P95 耗时: " . number_format($p95Time, 2) . " 毫秒\n";
        echo "P99 耗时: " . number_format($p99Time, 2) . " 毫秒\n";
        echo "吞吐量: " . number_format($concurrentCount / $totalTime, 2) . " 操作/秒\n";
        
        // 验证只有 limit 个成功
        $this->assertEquals($limit, $successCount, "应该只有 {$limit} 个成功");
        // 性能断言：P95 耗时应该小于 2 毫秒（提高性能基准）
        // 注意：实际性能取决于网络延迟和 Redis 服务器性能
        $this->assertLessThan(2.0, $p95Time, 'P95 耗时应该小于 2 毫秒');
    }

    /**
     * 测试 heartbeat 性能
     */
    public function testHeartbeatPerformance()
    {
        $key = $this->generateTestKey('test:semaphore:perf:heartbeat');
        $limit = 10;
        $iterations = 1000;
        
        $semaphore = new IncrSemaphore(self::$redis, $key, $limit, 600);
        
        // 先获取信号量
        $semaphore->acquire();
        
        $startTime = microtime(true);
        
        for ($i = 0; $i < $iterations; $i++) {
            $semaphore->heartbeat();
        }
        
        $endTime = microtime(true);
        $totalTime = $endTime - $startTime;
        $avgTime = ($totalTime / $iterations) * 1000; // 转换为毫秒
        
        // 清理
        $semaphore->release();
        
        echo "\n=== Heartbeat 性能测试 ===\n";
        echo "操作数: {$iterations}\n";
        echo "总耗时: " . number_format($totalTime, 4) . " 秒\n";
        echo "平均耗时: " . number_format($avgTime, 2) . " 毫秒/次\n";
        echo "吞吐量: " . number_format($iterations / $totalTime, 2) . " 操作/秒\n";
        
        // 性能断言：平均耗时应该小于 1 毫秒（提高性能基准）
        // 注意：实际性能取决于网络延迟和 Redis 服务器性能
        $this->assertLessThan(1.0, $avgTime, '平均耗时应该小于 1 毫秒');
    }

    /**
     * 测试长时间运行的稳定性（1000 次循环）
     */
    public function testLongRunningStability()
    {
        $key = $this->generateTestKey('test:semaphore:perf:stability');
        $limit = 5;
        $cycles = 1000;
        
        $semaphore = new IncrSemaphore(self::$redis, $key, $limit, 600);
        
        $startTime = microtime(true);
        $errors = 0;
        
        for ($i = 0; $i < $cycles; $i++) {
            try {
                $acquired = $semaphore->acquire();
                if ($acquired) {
                    // 模拟一些处理时间
                    usleep(1000); // 1ms
                    $semaphore->release();
                }
            } catch (\Exception $e) {
                $errors++;
            }
        }
        
        $endTime = microtime(true);
        $totalTime = $endTime - $startTime;
        
        echo "\n=== 长时间运行稳定性测试 ===\n";
        echo "循环次数: {$cycles}\n";
        echo "错误数: {$errors}\n";
        echo "总耗时: " . number_format($totalTime, 4) . " 秒\n";
        echo "平均耗时: " . number_format(($totalTime / $cycles) * 1000, 2) . " 毫秒/循环\n";
        
        // 稳定性断言：不应该有错误
        $this->assertEquals(0, $errors, '长时间运行不应该有错误');
    }

    /**
     * 测试 IncrSemaphore vs HashSemaphore 性能对比
     */
    public function testIncrVsHashPerformanceComparison()
    {
        $incrKey = $this->generateTestKey('test:semaphore:perf:incr:compare');
        $hashKey = $this->generateTestKey('test:semaphore:perf:hash:compare');
        $limit = 100;
        $iterations = 500;
        
        // 测试 IncrSemaphore
        $incrSemaphore = new IncrSemaphore(self::$redis, $incrKey, $limit, 600);
        $incrStart = microtime(true);
        $incrSuccess = 0;
        for ($i = 0; $i < $iterations; $i++) {
            if ($incrSemaphore->acquire()) {
                $incrSuccess++;
            }
        }
        $incrEnd = microtime(true);
        $incrTime = $incrEnd - $incrStart;
        
        // 清理 IncrSemaphore
        for ($i = 0; $i < $incrSuccess; $i++) {
            $incrSemaphore->release();
        }
        
        // 测试 HashSemaphore
        $hashSemaphores = [];
        $hashStart = microtime(true);
        $hashSuccess = 0;
        for ($i = 0; $i < $iterations; $i++) {
            $hashSemaphore = new HashSemaphore(self::$redis, $hashKey, $limit, 600);
            if ($hashSemaphore->acquire()) {
                $hashSuccess++;
                $hashSemaphores[] = $hashSemaphore;
            }
        }
        $hashEnd = microtime(true);
        $hashTime = $hashEnd - $hashStart;
        
        // 清理 HashSemaphore
        foreach ($hashSemaphores as $semaphore) {
            $semaphore->release();
        }
        
        $incrAvg = ($incrTime / $iterations) * 1000;
        $hashAvg = ($hashTime / $iterations) * 1000;
        
        echo "\n=== IncrSemaphore vs HashSemaphore 性能对比 ===\n";
        echo "操作数: {$iterations}\n";
        echo "IncrSemaphore:\n";
        echo "  - 总耗时: " . number_format($incrTime, 4) . " 秒\n";
        echo "  - 平均耗时: " . number_format($incrAvg, 2) . " 毫秒/次\n";
        echo "  - 吞吐量: " . number_format($iterations / $incrTime, 2) . " 操作/秒\n";
        echo "HashSemaphore:\n";
        echo "  - 总耗时: " . number_format($hashTime, 4) . " 秒\n";
        echo "  - 平均耗时: " . number_format($hashAvg, 2) . " 毫秒/次\n";
        echo "  - 吞吐量: " . number_format($iterations / $hashTime, 2) . " 操作/秒\n";
        echo "性能差异: " . number_format((($hashAvg - $incrAvg) / $incrAvg) * 100, 2) . "%\n";
        
        // 验证两种实现都能正常工作
        $this->assertGreaterThan(0, $incrSuccess, 'IncrSemaphore 应该成功');
        $this->assertGreaterThan(0, $hashSuccess, 'HashSemaphore 应该成功');
    }

    /**
     * 测试连续快速 acquire-release 性能
     */
    public function testRapidAcquireReleasePerformance()
    {
        $key = $this->generateTestKey('test:semaphore:perf:rapid');
        $limit = 1;
        $iterations = 200;
        
        $semaphore = new IncrSemaphore(self::$redis, $key, $limit, 600);
        
        $startTime = microtime(true);
        $times = [];
        
        for ($i = 0; $i < $iterations; $i++) {
            $opStart = microtime(true);
            $semaphore->acquire();
            $semaphore->release();
            $opEnd = microtime(true);
            $times[] = ($opEnd - $opStart) * 1000; // 毫秒
        }
        
        $endTime = microtime(true);
        $totalTime = $endTime - $startTime;
        $avgTime = array_sum($times) / count($times);
        $minTime = min($times);
        $maxTime = max($times);
        
        echo "\n=== 快速 Acquire-Release 性能测试 ===\n";
        echo "操作数: {$iterations}\n";
        echo "总耗时: " . number_format($totalTime, 4) . " 秒\n";
        echo "平均耗时: " . number_format($avgTime, 2) . " 毫秒/次\n";
        echo "最小耗时: " . number_format($minTime, 2) . " 毫秒\n";
        echo "最大耗时: " . number_format($maxTime, 2) . " 毫秒\n";
        echo "吞吐量: " . number_format($iterations / $totalTime, 2) . " 操作/秒\n";
        
        // 性能断言：平均耗时应该小于 3 毫秒（快速连续操作，提高性能基准）
        // 注意：实际性能取决于网络延迟和 Redis 服务器性能
        $this->assertLessThan(3.0, $avgTime, '平均耗时应该小于 3 毫秒');
    }

    /**
     * 测试 HashSemaphore 和 IncrSemaphore 完整获取和释放操作的平均耗时对比
     */
    public function testAcquireReleaseAverageTimeComparison()
    {
        $incrKey = $this->generateTestKey('test:semaphore:perf:incr:acquire:release');
        $hashKey = $this->generateTestKey('test:semaphore:perf:hash:acquire:release');
        $limit = 10;
        $iterations = 1000;
        
        // 测试 IncrSemaphore 完整获取和释放操作
        $incrSemaphore = new IncrSemaphore(self::$redis, $incrKey, $limit, 600);
        $incrTimes = [];
        $incrStart = microtime(true);
        
        for ($i = 0; $i < $iterations; $i++) {
            $opStart = microtime(true);
            if ($incrSemaphore->acquire()) {
                $incrSemaphore->release();
            }
            $opEnd = microtime(true);
            $incrTimes[] = ($opEnd - $opStart) * 1000; // 转换为毫秒
        }
        
        $incrEnd = microtime(true);
        $incrTotalTime = $incrEnd - $incrStart;
        $incrAvgTime = array_sum($incrTimes) / count($incrTimes);
        $incrMinTime = min($incrTimes);
        $incrMaxTime = max($incrTimes);
        
        // 测试 HashSemaphore 完整获取和释放操作
        $hashTimes = [];
        $hashStart = microtime(true);
        
        for ($i = 0; $i < $iterations; $i++) {
            $opStart = microtime(true);
            $hashSemaphore = new HashSemaphore(self::$redis, $hashKey, $limit, 600);
            if ($hashSemaphore->acquire()) {
                $hashSemaphore->release();
            }
            $opEnd = microtime(true);
            $hashTimes[] = ($opEnd - $opStart) * 1000; // 转换为毫秒
        }
        
        $hashEnd = microtime(true);
        $hashTotalTime = $hashEnd - $hashStart;
        $hashAvgTime = array_sum($hashTimes) / count($hashTimes);
        $hashMinTime = min($hashTimes);
        $hashMaxTime = max($hashTimes);
        
        // 计算性能差异百分比
        $performanceDiff = (($hashAvgTime - $incrAvgTime) / $incrAvgTime) * 100;
        
        echo "\n=== HashSemaphore vs IncrSemaphore 完整获取和释放操作平均耗时对比 ===\n";
        echo "测试次数: {$iterations}\n";
        echo "Limit: {$limit}\n";
        echo "\nIncrSemaphore:\n";
        echo "  - 总耗时: " . number_format($incrTotalTime, 4) . " 秒\n";
        echo "  - 平均耗时: " . number_format($incrAvgTime, 4) . " 毫秒/次（完整获取+释放）\n";
        echo "  - 最小耗时: " . number_format($incrMinTime, 4) . " 毫秒\n";
        echo "  - 最大耗时: " . number_format($incrMaxTime, 4) . " 毫秒\n";
        echo "  - 吞吐量: " . number_format($iterations / $incrTotalTime, 2) . " 操作/秒\n";
        echo "\nHashSemaphore:\n";
        echo "  - 总耗时: " . number_format($hashTotalTime, 4) . " 秒\n";
        echo "  - 平均耗时: " . number_format($hashAvgTime, 4) . " 毫秒/次（完整获取+释放）\n";
        echo "  - 最小耗时: " . number_format($hashMinTime, 4) . " 毫秒\n";
        echo "  - 最大耗时: " . number_format($hashMaxTime, 4) . " 毫秒\n";
        echo "  - 吞吐量: " . number_format($iterations / $hashTotalTime, 2) . " 操作/秒\n";
        echo "\n性能对比:\n";
        echo "  - 性能差异: " . number_format($performanceDiff, 2) . "%\n";
        if ($performanceDiff > 0) {
            echo "  - IncrSemaphore 比 HashSemaphore 快 " . number_format($performanceDiff, 2) . "%\n";
        } else {
            echo "  - HashSemaphore 比 IncrSemaphore 快 " . number_format(abs($performanceDiff), 2) . "%\n";
        }
        
        // 验证两种实现都能正常工作
        $this->assertGreaterThan(0, $incrAvgTime, 'IncrSemaphore 平均耗时应该大于 0');
        $this->assertGreaterThan(0, $hashAvgTime, 'HashSemaphore 平均耗时应该大于 0');
    }

    /**
     * 计算百分位数
     */
    private function percentile(array $values, float $percentile): float
    {
        sort($values);
        $index = (int)ceil(($percentile / 100) * count($values)) - 1;
        $index = max(0, min($index, count($values) - 1));
        return $values[$index];
    }
}
