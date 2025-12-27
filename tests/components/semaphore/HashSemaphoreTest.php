<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components\semaphore;

use mikemadisonweb\rabbitmq\components\semaphore\HashSemaphore;
use mikemadisonweb\rabbitmq\tests\TestCase;
use yii\redis\Connection;

class HashSemaphoreTest extends TestCase
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

        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
        $result = $semaphore->acquire();

        $this->assertTrue($result);
    }

    public function testAcquireLimitReached()
    {
        $redis = $this->createRedisMock();
        // 模拟超过限制，返回 0
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(0); // 达到限制，获取失败

        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
        $result = $semaphore->acquire();

        $this->assertFalse($result);
    }

    public function testAcquireSetsExpire()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->callback(function ($args) {
                // args[0] 是 lua 脚本，args[1] 是 numKeys，args[2] 是 key（展开），args[3] 是 token，args[4] 是 limit，args[5] 是 ttl
                return is_string($args[0]) && 
                       strpos($args[0], 'EXPIRE') !== false &&
                       $args[1] === 1 &&
                       $args[2] === 'test:key' &&
                       is_string($args[3]) && // token
                       $args[4] === 10 &&
                       $args[5] === 600;
            }))
            ->willReturn(1);

        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
        $semaphore->acquire();
    }

    public function testAcquireUsesUniqueToken()
    {
        // 这个测试主要验证 token 的唯一性，不需要实际调用 Redis
        $redis = $this->createRedisMock();
        
        $semaphore1 = new HashSemaphore($redis, 'test:key', 10, 600);
        $semaphore2 = new HashSemaphore($redis, 'test:key', 10, 600);

        $token1 = $this->getInaccessibleProperty($semaphore1, 'token');
        $token2 = $this->getInaccessibleProperty($semaphore2, 'token');

        // 两个实例应该有不同的 token
        $this->assertNotEquals($token1, $token2);
        $this->assertNotEmpty($token1);
        $this->assertNotEmpty($token2);
    }

    public function testReleaseSuccess()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(1); // 成功移除

        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
        $semaphore->release();
    }

    public function testReleaseTokenNotExists()
    {
        $redis = $this->createRedisMock();
        // 模拟 token 不存在的情况，Lua 脚本会检查 SISMEMBER 并返回 0
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(0);

        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
        $semaphore->release(); // 应该不会抛出异常
    }

    public function testReleaseSetsExpireWhenSetNotEmpty()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->callback(function ($args) {
                // args[0] 是 lua 脚本，args[1] 是 numKeys，args[2] 是 key（展开），args[3] 是 token，args[4] 是 ttl
                return is_string($args[0]) && 
                       strpos($args[0], 'EXPIRE') !== false &&
                       $args[1] === 1 &&
                       $args[2] === 'test:key' &&
                       is_string($args[3]) && // token
                       $args[4] === 600;
            }))
            ->willReturn(1);

        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
        $semaphore->release();
    }

    public function testHeartbeatSuccess()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->callback(function ($args) {
                // args[0] 是 lua 脚本，args[1] 是 numKeys，args[2] 是 key（展开），args[3] 是 token，args[4] 是 ttl
                return is_string($args[0]) && 
                       strpos($args[0], 'EXPIRE') !== false &&
                       $args[1] === 1 &&
                       $args[2] === 'test:key' &&
                       is_string($args[3]) && // token
                       $args[4] === 600;
            }))
            ->willReturn(1); // 续期成功

        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
        $semaphore->heartbeat();
    }

    public function testHeartbeatTokenNotExists()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(0); // token 不存在

        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
        $semaphore->heartbeat(); // 应该不会抛出异常
    }

    public function testGenToken()
    {
        $tokens = [];
        for ($i = 0; $i < 100; $i++) {
            $token = HashSemaphore::genToken();
            // 验证 token 格式：应该是 base64 编码的 12 字节，去掉填充后应该是 16 字符
            $this->assertEquals(16, strlen($token));
            $this->assertStringNotContainsString('=', $token);
            $this->assertStringNotContainsString('+', $token);
            $this->assertStringNotContainsString('/', $token);
            // 验证唯一性
            $this->assertNotContains($token, $tokens, "Token should be unique, but found duplicate at iteration $i");
            $tokens[] = $token;
        }
    }

    public function testGenTokenUniqueness()
    {
        $tokens = [];
        for ($i = 0; $i < 1000; $i++) {
            $token = HashSemaphore::genToken();
            $this->assertNotContains($token, $tokens, "Token should be unique, but found duplicate at iteration $i");
            $tokens[] = $token;
        }
    }

    public function testConstructor()
    {
        $redis = $this->createRedisMock();
        $semaphore = new HashSemaphore($redis, 'test:key', 5, 300, 30);

        // 验证属性设置
        $this->assertEquals('test:key', $this->getInaccessibleProperty($semaphore, 'key'));
        $this->assertEquals(5, $this->getInaccessibleProperty($semaphore, 'limit'));
        $this->assertEquals(300, $this->getInaccessibleProperty($semaphore, 'ttl'));
        $this->assertEquals(30, $this->getInaccessibleProperty($semaphore, 'acquireSleep'));
        
        // 验证 token 已生成
        $token = $this->getInaccessibleProperty($semaphore, 'token');
        $this->assertNotEmpty($token);
        $this->assertEquals(16, strlen($token));
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

        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600, 0); // acquireSleep = 0
        $result = $semaphore->acquire_wait();

        $this->assertTrue($result);
    }

    public function testReleaseUsesCorrectToken()
    {
        $redis = $this->createRedisMock();
        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
        $token = $this->getInaccessibleProperty($semaphore, 'token');

        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->callback(function ($args) use ($token) {
                // args[0] 是 lua 脚本，args[1] 是 numKeys，args[2] 是 key（展开），args[3] 是 token，args[4] 是 ttl
                return is_string($args[0]) && 
                       strpos($args[0], 'SREM') !== false &&
                       $args[1] === 1 &&
                       $args[2] === 'test:key' &&
                       $args[3] === $token &&
                       $args[4] === 600;
            }))
            ->willReturn(1);

        $semaphore->release();
    }

    public function testHeartbeatUsesCorrectToken()
    {
        $redis = $this->createRedisMock();
        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
        $token = $this->getInaccessibleProperty($semaphore, 'token');

        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->callback(function ($args) use ($token) {
                // args[0] 是 lua 脚本，args[1] 是 numKeys，args[2] 是 key（展开），args[3] 是 token，args[4] 是 ttl
                return is_string($args[0]) && 
                       strpos($args[0], 'SISMEMBER') !== false &&
                       $args[1] === 1 &&
                       $args[2] === 'test:key' &&
                       $args[3] === $token &&
                       $args[4] === 600;
            }))
            ->willReturn(1);

        $semaphore->heartbeat();
    }

    public function testAcquireWithNullReturn()
    {
        $redis = $this->createRedisMock();
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn(null); // 返回 null

        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
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

        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
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

        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
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
        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
        $token = $this->getInaccessibleProperty($semaphore, 'token');
        
        $redis->expects($this->exactly(3))
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturnOnConsecutiveCalls(
                1,  // 第一次获取成功
                1,  // 释放成功
                1   // 再次获取成功
            );

        // 第一次获取
        $result1 = $semaphore->acquire();
        $this->assertTrue($result1, '第一次获取应该成功');
        
        // 释放
        $semaphore->release();
        
        // 再次获取（使用新的 token）
        $semaphore2 = new HashSemaphore($redis, 'test:key', 10, 600);
        $result2 = $semaphore2->acquire();
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

        $semaphores = [];
        for ($i = 0; $i < $limit + 1; $i++) {
            $semaphores[] = new HashSemaphore($redis, 'test:key', $limit, 600);
        }
        
        // 连续获取直到达到 limit
        for ($i = 0; $i < $limit; $i++) {
            $result = $semaphores[$i]->acquire();
            $this->assertTrue($result, "第 " . ($i + 1) . " 次获取应该成功");
        }
        
        // 达到 limit 后应该无法获取
        $result = $semaphores[$limit]->acquire();
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
                1,              // 释放成功
                1               // 释放后可以再次获取
            );

        $semaphores = [];
        for ($i = 0; $i < $limit + 1; $i++) {
            $semaphores[] = new HashSemaphore($redis, 'test:key', $limit, 600);
        }
        
        // 连续获取直到达到 limit
        for ($i = 0; $i < $limit; $i++) {
            $result = $semaphores[$i]->acquire();
            $this->assertTrue($result, "第 " . ($i + 1) . " 次获取应该成功");
        }
        
        // 达到 limit 后应该无法获取
        $result = $semaphores[$limit]->acquire();
        $this->assertFalse($result, '达到 limit 后应该无法获取');
        
        // 释放第一个
        $semaphores[0]->release();
        
        // 释放后应该可以再次获取
        $result = $semaphores[$limit]->acquire();
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
                1,  // 释放成功
                1   // 释放后可以再次获取
            );

        $semaphore1 = new HashSemaphore($redis, 'test:key', 1, 600);
        $semaphore2 = new HashSemaphore($redis, 'test:key', 1, 600);
        $semaphore3 = new HashSemaphore($redis, 'test:key', 1, 600);
        
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
            ->willReturnCallback(function () {
                static $callCount = 0;
                $callCount++;
                // 奇数调用是 acquire，偶数调用是 release
                return 1; // 都返回成功
            });

        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
        
        // 执行多次获取-释放循环
        for ($i = 0; $i < $cycles; $i++) {
            $result = $semaphore->acquire();
            $this->assertTrue($result, "第 " . ($i + 1) . " 次循环获取应该成功");
            
            $semaphore->release();
        }
    }

    /**
     * 测试同一个 semaphore 实例不能重复获取（HashSemaphore 的特性）
     * 注意：IncrSemaphore 没有这个限制，但 HashSemaphore 有
     */
    public function testSameInstanceCannotAcquireTwice()
    {
        $redis = $this->createRedisMock();
        $semaphore = new HashSemaphore($redis, 'test:key', 10, 600);
        $token = $this->getInaccessibleProperty($semaphore, 'token');
        
        $redis->expects($this->exactly(2))
            ->method('__call')
            ->with('eval', $this->callback(function ($args) use ($token) {
                // 验证使用的是同一个 token（args[3] 是展开的 token）
                return $args[3] === $token;
            }))
            ->willReturnOnConsecutiveCalls(
                1,  // 第一次获取成功
                0   // 第二次获取失败（token 已存在）
            );

        // 第一次获取应该成功
        $result1 = $semaphore->acquire();
        $this->assertTrue($result1, '第一次获取应该成功');
        
        // 同一个实例再次获取应该失败（因为 token 已存在）
        $result2 = $semaphore->acquire();
        $this->assertFalse($result2, '同一个实例再次获取应该失败');
    }
}

