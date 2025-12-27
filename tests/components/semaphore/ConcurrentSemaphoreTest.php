<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components\semaphore;

use mikemadisonweb\rabbitmq\components\semaphore\HashSemaphore;
use mikemadisonweb\rabbitmq\components\semaphore\IncrSemaphore;
use mikemadisonweb\rabbitmq\tests\TestCase;
use yii\redis\Connection;

/**
 * 并发场景测试说明
 * 
 * 注意：由于单元测试环境的限制，真正的并发测试需要：
 * 1. 真实的 Redis 连接（不是 mock）
 * 2. 多个进程或线程同时操作
 * 3. 或者使用 PHP 的并发测试工具（如 ReactPHP、Swoole 等）
 * 
 * 当前测试通过以下方式模拟并发场景：
 * 1. 创建多个 semaphore 实例（模拟多个消费者）
 * 2. 通过 mock Redis 预设返回值来模拟并发操作的结果
 * 3. 验证 limit 限制的正确性
 * 
 * 真正的并发测试建议：
 * - 使用集成测试环境，连接真实的 Redis
 * - 使用多进程或多线程同时执行 acquire 操作
 * - 验证实际获取成功的数量不超过 limit
 */
class ConcurrentSemaphoreTest extends TestCase
{
    private function createRedisMock()
    {
        $redis = $this->getMockBuilder(Connection::class)
            ->setMethods(['getIsActive', '__call'])
            ->getMock();
        $redis->method('getIsActive')->willReturn(true);
        return $redis;
    }

    /**
     * 测试多个 IncrSemaphore 实例同时获取（模拟并发场景）
     * 
     * 注意：这是通过 mock 模拟的并发场景，不是真正的并发测试
     * 真正的并发测试需要使用真实的 Redis 连接和多个进程/线程
     */
    public function testIncrSemaphoreConcurrentAcquire()
    {
        $redis = $this->createRedisMock();
        $limit = 5;
        $concurrentCount = 10; // 模拟 10 个并发请求
        
        // 模拟并发场景：前 5 个成功，后 5 个失败
        $returnValues = array_merge(
            array_fill(0, $limit, 1),      // 前 5 次返回成功
            array_fill(0, $concurrentCount - $limit, 0) // 后 5 次返回失败
        );
        
        $redis->expects($this->exactly($concurrentCount))
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturnOnConsecutiveCalls(...$returnValues);

        $semaphores = [];
        $results = [];
        
        // 创建多个 semaphore 实例（模拟多个消费者同时获取）
        for ($i = 0; $i < $concurrentCount; $i++) {
            $semaphores[] = new IncrSemaphore($redis, 'test:key', $limit, 600);
        }
        
        // 顺序执行 acquire（在实际并发场景中，这些操作会同时发生）
        for ($i = 0; $i < $concurrentCount; $i++) {
            $results[] = $semaphores[$i]->acquire();
        }
        
        // 验证：前 limit 个应该成功，后面的应该失败
        $successCount = 0;
        $failCount = 0;
        for ($i = 0; $i < $concurrentCount; $i++) {
            if ($results[$i]) {
                $successCount++;
                $this->assertLessThanOrEqual($limit, $successCount, "成功数量不应超过 limit");
            } else {
                $failCount++;
            }
        }
        
        $this->assertEquals($limit, $successCount, "应该有 {$limit} 个成功获取");
        $this->assertEquals($concurrentCount - $limit, $failCount, "应该有 " . ($concurrentCount - $limit) . " 个失败");
    }

    /**
     * 测试多个 HashSemaphore 实例同时获取（模拟并发场景）
     * 
     * 注意：这是通过 mock 模拟的并发场景，不是真正的并发测试
     */
    public function testHashSemaphoreConcurrentAcquire()
    {
        $redis = $this->createRedisMock();
        $limit = 5;
        $concurrentCount = 10; // 模拟 10 个并发请求
        
        // 模拟并发场景：前 5 个成功，后 5 个失败
        $returnValues = array_merge(
            array_fill(0, $limit, 1),      // 前 5 次返回成功
            array_fill(0, $concurrentCount - $limit, 0) // 后 5 次返回失败
        );
        
        $redis->expects($this->exactly($concurrentCount))
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturnOnConsecutiveCalls(...$returnValues);

        $semaphores = [];
        $results = [];
        
        // 创建多个 semaphore 实例（每个实例有唯一的 token）
        for ($i = 0; $i < $concurrentCount; $i++) {
            $semaphores[] = new HashSemaphore($redis, 'test:key', $limit, 600);
        }
        
        // 顺序执行 acquire（在实际并发场景中，这些操作会同时发生）
        for ($i = 0; $i < $concurrentCount; $i++) {
            $results[] = $semaphores[$i]->acquire();
        }
        
        // 验证：前 limit 个应该成功，后面的应该失败
        $successCount = 0;
        $failCount = 0;
        for ($i = 0; $i < $concurrentCount; $i++) {
            if ($results[$i]) {
                $successCount++;
                $this->assertLessThanOrEqual($limit, $successCount, "成功数量不应超过 limit");
            } else {
                $failCount++;
            }
        }
        
        $this->assertEquals($limit, $successCount, "应该有 {$limit} 个成功获取");
        $this->assertEquals($concurrentCount - $limit, $failCount, "应该有 " . ($concurrentCount - $limit) . " 个失败");
    }

    /**
     * 测试并发场景：达到 limit 后释放，其他实例可以获取
     * 
     * 模拟场景：
     * 1. 5 个实例同时获取，前 3 个成功（limit=3）
     * 2. 释放一个
     * 3. 剩余 2 个实例可以获取
     */
    public function testConcurrentAcquireReleaseAcquire()
    {
        $redis = $this->createRedisMock();
        $limit = 3;
        $totalInstances = 5;
        
        $redis->expects($this->exactly(7))
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturnOnConsecutiveCalls(
                1, 1, 1,        // 前 3 个实例获取成功
                0, 0,          // 后 2 个实例获取失败（达到 limit）
                1,              // 释放第一个实例
                1               // 释放后，第 4 个实例可以获取
            );

        $semaphores = [];
        for ($i = 0; $i < $totalInstances; $i++) {
            $semaphores[] = new HashSemaphore($redis, 'test:key', $limit, 600);
        }
        
        // 前 3 个获取成功
        for ($i = 0; $i < $limit; $i++) {
            $result = $semaphores[$i]->acquire();
            $this->assertTrue($result, "第 " . ($i + 1) . " 个实例应该成功获取");
        }
        
        // 后 2 个获取失败
        for ($i = $limit; $i < $totalInstances; $i++) {
            $result = $semaphores[$i]->acquire();
            $this->assertFalse($result, "第 " . ($i + 1) . " 个实例应该获取失败");
        }
        
        // 释放第一个
        $semaphores[0]->release();
        
        // 释放后，第 4 个实例可以获取
        $result = $semaphores[$limit]->acquire();
        $this->assertTrue($result, '释放后，第 4 个实例应该可以获取');
    }

    /**
     * 测试并发场景：多个实例同时释放，然后可以重新获取
     */
    public function testConcurrentReleaseAndReacquire()
    {
        $redis = $this->createRedisMock();
        $limit = 3;
        $acquiredCount = 3;
        
        $redis->expects($this->exactly(9))
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturnOnConsecutiveCalls(
                1, 1, 1,        // 3 个实例获取成功
                1, 1, 1,        // 3 个实例释放成功
                1, 1, 1         // 释放后，3 个新实例可以重新获取
            );

        // 第一轮：获取
        $semaphores1 = [];
        for ($i = 0; $i < $acquiredCount; $i++) {
            $semaphores1[] = new HashSemaphore($redis, 'test:key', $limit, 600);
            $result = $semaphores1[$i]->acquire();
            $this->assertTrue($result, "第 " . ($i + 1) . " 个实例应该成功获取");
        }
        
        // 释放所有
        foreach ($semaphores1 as $semaphore) {
            $semaphore->release();
        }
        
        // 第二轮：重新获取
        $semaphores2 = [];
        for ($i = 0; $i < $acquiredCount; $i++) {
            $semaphores2[] = new HashSemaphore($redis, 'test:key', $limit, 600);
            $result = $semaphores2[$i]->acquire();
            $this->assertTrue($result, "释放后，第 " . ($i + 1) . " 个新实例应该可以获取");
        }
    }
}

