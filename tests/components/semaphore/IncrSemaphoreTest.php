<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components\semaphore;

use mikemadisonweb\rabbitmq\components\semaphore\IncrSemaphore;
use mikemadisonweb\rabbitmq\tests\TestCase;
use yii\redis\Connection;

class IncrSemaphoreTest extends TestCase
{
    private function createRedisMock()
    {
        $redis = $this->getMockBuilder(Connection::class)
            ->setMethods(['getIsActive', '__call'])
            ->getMock();
        $redis->method('getIsActive')->willReturn(true);
        return $redis;
    }

    public function testAcquireSuccess()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(1); // 成功获取

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600);
        $result = $semaphore->acquire();

        $this->assertTrue($result);
    }

    public function testAcquireLimitReached()
    {
        $redis = $this->createRedisMock();
        // 模拟当前值已经达到限制，返回 0
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(0); // 达到限制，获取失败

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600);
        $result = $semaphore->acquire();

        $this->assertFalse($result);
    }

    public function testAcquireSetsExpire()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->callback(function ($args) {
                // args[0] 是 lua 脚本，args[1] 是 numKeys，args[2] 是 key（展开），args[3] 是 limit，args[4] 是 ttl
                return is_string($args[0]) && 
                       strpos($args[0], 'EXPIRE') !== false &&
                       $args[1] === 1 &&
                       $args[2] === 'test:key' &&
                       $args[3] === 10 &&
                       $args[4] === 600;
            }))
            ->willReturn(1);

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600);
        $semaphore->acquire();
    }

    public function testReleaseSuccess()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(5); // 释放后剩余 5

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600);
        $semaphore->release();
    }

    public function testReleaseKeyNotExists()
    {
        $redis = $this->createRedisMock();
        // 模拟 key 不存在的情况，Lua 脚本会检查 EXISTS 并返回 0
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(0);

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600);
        $semaphore->release(); // 应该不会抛出异常
    }

    public function testReleaseZeroValue()
    {
        $redis = $this->createRedisMock();
        // 模拟值为 0 的情况
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(0);

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600);
        $semaphore->release(); // 应该不会抛出异常
    }

    public function testReleaseSetsExpireWhenValueGreaterThanZero()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->callback(function ($args) {
                // args[0] 是 lua 脚本，args[1] 是 numKeys，args[2] 是 key（展开），args[3] 是 arg（展开）
                return is_string($args[0]) && 
                       strpos($args[0], 'EXPIRE') !== false &&
                       $args[1] === 1 &&
                       $args[2] === 'test:key' &&
                       $args[3] === 600;
            }))
            ->willReturn(5); // 释放后还有 5

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600);
        $semaphore->release();
    }

    public function testHeartbeatSuccess()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->callback(function ($args) {
                // args[0] 是 lua 脚本，args[1] 是 numKeys，args[2] 是 key（展开），args[3] 是 arg（展开）
                return is_string($args[0]) && 
                       strpos($args[0], 'EXPIRE') !== false &&
                       $args[1] === 1 &&
                       $args[2] === 'test:key' &&
                       $args[3] === 600;
            }))
            ->willReturn(1); // 续期成功

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600);
        $semaphore->heartbeat();
    }

    public function testHeartbeatKeyNotExists()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(0); // key 不存在或值为 0

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600);
        $semaphore->heartbeat(); // 应该不会抛出异常
    }

    public function testHeartbeatZeroValue()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(0); // 值为 0，不续期

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600);
        $semaphore->heartbeat(); // 应该不会抛出异常
    }

    public function testAcquireWait()
    {
        $redis = $this->createRedisMock();
        // 当 acquireSleep = 0 时，acquire_wait() 只会调用一次 acquire()
        // 如果第一次失败，不会重试（因为 acquireSleep = 0 时直接返回 acquire() 的结果）
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(1); // 成功获取

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600, 0); // acquireSleep = 0
        $result = $semaphore->acquire_wait();

        $this->assertTrue($result);
    }

    public function testConstructor()
    {
        $redis = $this->createRedisMock();
        $semaphore = new IncrSemaphore($redis, 'test:key', 5, 300, 30);

        // 验证属性设置
        $this->assertEquals('test:key', $this->getInaccessibleProperty($semaphore, 'key'));
        $this->assertEquals(5, $this->getInaccessibleProperty($semaphore, 'limit'));
        $this->assertEquals(300, $this->getInaccessibleProperty($semaphore, 'ttl'));
        $this->assertEquals(30, $this->getInaccessibleProperty($semaphore, 'acquireSleep'));
    }

    public function testAcquireWithNullReturn()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(null); // 返回 null

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600);
        $result = $semaphore->acquire();

        // null 应该被视为失败
        $this->assertFalse($result);
    }

    public function testReleaseWithNullReturn()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(null); // 返回 null

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600);
        // 返回 null 不应该抛出异常
        $semaphore->release();
    }

    public function testHeartbeatWithNullReturn()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(null); // 返回 null

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600);
        // 返回 null 不应该抛出异常
        $semaphore->heartbeat();
    }

    /**
     * 测试获取-释放-再获取的完整流程
     * 这是 semaphore 的核心功能，需要重点测试
     */
    public function testAcquireReleaseAcquireFlow()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->exactly(3))
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturnOnConsecutiveCalls(
                1,  // 第一次获取成功
                4,  // 释放后剩余 4
                1   // 再次获取成功
            );

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600);
        
        // 第一次获取
        $result1 = $semaphore->acquire();
        $this->assertTrue($result1, '第一次获取应该成功');
        
        // 释放
        $semaphore->release();
        
        // 再次获取
        $result2 = $semaphore->acquire();
        $this->assertTrue($result2, '释放后应该可以再次获取');
    }

    /**
     * 测试连续多次获取直到达到 limit
     */
    public function testMultipleAcquireUntilLimit()
    {
        $redis = $this->createRedisMock();
        $limit = 5;
        $redis->expects($this->exactly($limit + 1))
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturnOnConsecutiveCalls(
                1, 1, 1, 1, 1,  // 前 5 次获取成功
                0               // 第 6 次达到限制，获取失败
            );

        $semaphore = new IncrSemaphore($redis, 'test:key', $limit, 600);
        
        // 连续获取直到达到 limit
        for ($i = 0; $i < $limit; $i++) {
            $result = $semaphore->acquire();
            $this->assertTrue($result, "第 " . ($i + 1) . " 次获取应该成功");
        }
        
        // 达到 limit 后应该无法获取
        $result = $semaphore->acquire();
        $this->assertFalse($result, '达到 limit 后应该无法获取');
    }

    /**
     * 测试达到 limit 后释放一个，然后可以再次获取
     */
    public function testAcquireUntilLimitThenReleaseAndAcquire()
    {
        $redis = $this->createRedisMock();
        $limit = 3;
        $redis->expects($this->exactly(6))
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturnOnConsecutiveCalls(
                1, 1, 1,        // 前 3 次获取成功
                0,              // 第 4 次达到限制，获取失败
                2,              // 释放后剩余 2
                1               // 释放后可以再次获取
            );

        $semaphore = new IncrSemaphore($redis, 'test:key', $limit, 600);
        
        // 连续获取直到达到 limit
        for ($i = 0; $i < $limit; $i++) {
            $result = $semaphore->acquire();
            $this->assertTrue($result, "第 " . ($i + 1) . " 次获取应该成功");
        }
        
        // 达到 limit 后应该无法获取
        $result = $semaphore->acquire();
        $this->assertFalse($result, '达到 limit 后应该无法获取');
        
        // 释放一个
        $semaphore->release();
        
        // 释放后应该可以再次获取
        $result = $semaphore->acquire();
        $this->assertTrue($result, '释放后应该可以再次获取');
    }

    /**
     * 测试边界情况：limit = 1
     */
    public function testAcquireReleaseWithLimitOne()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->exactly(4))
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturnOnConsecutiveCalls(
                1,  // 第一次获取成功
                0,  // 第二次获取失败（limit = 1）
                0,  // 释放后剩余 0
                1   // 释放后可以再次获取
            );

        $semaphore = new IncrSemaphore($redis, 'test:key', 1, 600);
        
        // 第一次获取应该成功
        $result1 = $semaphore->acquire();
        $this->assertTrue($result1, 'limit=1 时第一次获取应该成功');
        
        // 第二次获取应该失败
        $result2 = $semaphore->acquire();
        $this->assertFalse($result2, 'limit=1 时第二次获取应该失败');
        
        // 释放
        $semaphore->release();
        
        // 释放后应该可以再次获取
        $result3 = $semaphore->acquire();
        $this->assertTrue($result3, '释放后应该可以再次获取');
    }

    /**
     * 测试多次获取和释放的循环
     */
    public function testMultipleAcquireReleaseCycles()
    {
        $redis = $this->createRedisMock();
        $cycles = 5;
        $redis->expects($this->exactly($cycles * 2))
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturnCallback(function () use ($cycles) {
                static $callCount = 0;
                $callCount++;
                // 奇数调用是 acquire，偶数调用是 release
                if ($callCount % 2 === 1) {
                    return 1; // acquire 成功
                } else {
                    return 3; // release 后剩余 3
                }
            });

        $semaphore = new IncrSemaphore($redis, 'test:key', 10, 600);
        
        // 执行多次获取-释放循环
        for ($i = 0; $i < $cycles; $i++) {
            $result = $semaphore->acquire();
            $this->assertTrue($result, "第 " . ($i + 1) . " 次循环获取应该成功");
            
            $semaphore->release();
        }
    }
}

