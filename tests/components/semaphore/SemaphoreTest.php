<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components\semaphore;

use mikemadisonweb\rabbitmq\components\Logger;
use mikemadisonweb\rabbitmq\components\semaphore\Semaphore;
use mikemadisonweb\rabbitmq\tests\TestCase;
use yii\redis\Connection;
use yii\redis\SocketException;

class SemaphoreTest extends TestCase
{
    /**
     * 创建一个用于测试的 Semaphore 实现
     */
    private function createTestSemaphore(Connection $redis, string $key, int $limit, int $ttl = 600, int $acquireSleep = 0, Logger $logger = null): Semaphore
    {
        // 如果没有提供 Logger，使用静默 Logger
        if ($logger === null) {
            $logger = $this->createSilentLogger();
        }
        
        // 使用 getMockForAbstractClass 创建抽象类的实例
        $semaphore = $this->getMockForAbstractClass(
            Semaphore::class,
            [$redis, $key, $limit, $logger, $ttl, $acquireSleep]
        );

        // Mock 抽象方法
        $semaphore->expects($this->any())
            ->method('acquire')
            ->willReturn(true);

        $semaphore->expects($this->any())
            ->method('release');

        $semaphore->expects($this->any())
            ->method('heartbeat');

        return $semaphore;
    }

    public function testEvalLua()
    {
        $redis = $this->getMockBuilder(Connection::class)
            ->setMethods(['getIsActive', '__call'])
            ->getMock();
        $redis->expects($this->once())
            ->method('getIsActive')
            ->willReturn(true);
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', ['return 1', 1, 'key1'])
            ->willReturn(1);

        $semaphore = $this->createTestSemaphore($redis, 'test:key', 10);
        $result = $this->invokeMethod($semaphore, 'evalLua', ['return 1', ['key1'], []]);

        $this->assertEquals(1, $result);
    }

    public function testEvalLuaWithArgs()
    {
        $redis = $this->getMockBuilder(Connection::class)
            ->setMethods(['getIsActive', '__call'])
            ->getMock();
        $redis->expects($this->once())
            ->method('getIsActive')
            ->willReturn(true);
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', ['return ARGV[1]', 1, 'key1', 'value1'])
            ->willReturn('value1');

        $semaphore = $this->createTestSemaphore($redis, 'test:key', 10);
        $result = $this->invokeMethod($semaphore, 'evalLua', ['return ARGV[1]', ['key1'], ['value1']]);

        $this->assertEquals('value1', $result);
    }

    public function testEvalLuaEmptyScript()
    {
        $redis = $this->createMock(Connection::class);
        $semaphore = $this->createTestSemaphore($redis, 'test:key', 10);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Lua script cannot be empty');
        $this->invokeMethod($semaphore, 'evalLua', ['', ['key1'], []]);
    }

    public function testExecuteWithRetrySuccess()
    {
        $redis = $this->getMockBuilder(Connection::class)
            ->setMethods(['getIsActive', '__call'])
            ->getMock();
        $redis->expects($this->once())
            ->method('getIsActive')
            ->willReturn(true);
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturn('success');

        $semaphore = $this->createTestSemaphore($redis, 'test:key', 10);
        $result = $this->invokeMethod($semaphore, 'executeWithRetry', [
            function () use ($redis) {
                return $redis->eval('return "success"', 0, [], []);
            }
        ]);

        $this->assertEquals('success', $result);
    }

    public function testExecuteWithRetryConnectionFailure()
    {
        $redis = $this->getMockBuilder(Connection::class)
            ->setMethods(['getIsActive', 'open', '__call'])
            ->getMock();
        $redis->expects($this->exactly(3))
            ->method('getIsActive')
            ->willReturn(false);
        $redis->expects($this->exactly(3))
            ->method('open');
        $redis->expects($this->exactly(3))
            ->method('__call')
            ->with('eval', $this->anything())
            ->willThrowException(new SocketException('Connection failed'));

        $semaphore = $this->createTestSemaphore($redis, 'test:key', 10);
        
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Redis semaphore operation failed after 3 attempts');
        $this->invokeMethod($semaphore, 'executeWithRetry', [
            function () use ($redis) {
                return $redis->eval('return "success"', 0, [], []);
            }
        ]);
    }

    public function testExecuteWithRetryWithReconnection()
    {
        $redis = $this->getMockBuilder(Connection::class)
            ->setMethods(['getIsActive', 'open', '__call'])
            ->getMock();
        $redis->expects($this->exactly(2))
            ->method('getIsActive')
            ->willReturnOnConsecutiveCalls(false, true);
        $redis->expects($this->once())
            ->method('open');
        $redis->expects($this->exactly(2))
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new SocketException('Connection failed')),
                'success'
            );

        $semaphore = $this->createTestSemaphore($redis, 'test:key', 10);
        $result = $this->invokeMethod($semaphore, 'executeWithRetry', [
            function () use ($redis) {
                return $redis->eval('return "success"', 0, [], []);
            }
        ]);

        $this->assertEquals('success', $result);
    }

    public function testEnsureConnection()
    {
        $redis = $this->createMock(Connection::class);
        $redis->expects($this->once())
            ->method('getIsActive')
            ->willReturn(false);
        $redis->expects($this->once())
            ->method('open');

        $semaphore = $this->createTestSemaphore($redis, 'test:key', 10);
        $this->invokeMethod($semaphore, 'ensureConnection', []);
    }

    public function testEnsureConnectionAlreadyActive()
    {
        $redis = $this->createMock(Connection::class);
        $redis->expects($this->once())
            ->method('getIsActive')
            ->willReturn(true);
        $redis->expects($this->never())
            ->method('open');

        $semaphore = $this->createTestSemaphore($redis, 'test:key', 10);
        $this->invokeMethod($semaphore, 'ensureConnection', []);
    }

    public function testAcquireWaitWithoutSleep()
    {
        $redis = $this->createMock(Connection::class);
        $semaphore = $this->createTestSemaphore($redis, 'test:key', 10, 600, 0);
        
        // acquire_wait 会调用 acquire，由于 acquireSleep = 0，应该只调用一次
        $result = $semaphore->acquire_wait();
        $this->assertTrue($result);
    }

    public function testAcquireWaitWithSleep()
    {
        $redis = $this->createMock(Connection::class);
        $logger = $this->createSilentLogger();
        $semaphore = $this->getMockForAbstractClass(
            Semaphore::class,
            [$redis, 'test:key', 10, $logger, 600, 1] // acquireSleep = 1，会循环重试
        );

        // 第一次失败，第二次成功
        $semaphore->expects($this->exactly(2))
            ->method('acquire')
            ->willReturnOnConsecutiveCalls(false, true);

        // 由于 acquireSleep > 0，会循环重试，但为了避免测试时间过长，
        // 我们使用 mock，实际不会 sleep
        // 注意：由于 sleep() 是 PHP 内置函数，无法直接 mock，所以这个测试
        // 实际上会 sleep(1)，但为了测试逻辑正确性，我们接受这个代价
        $result = $semaphore->acquire_wait();
        $this->assertTrue($result);
    }

    public function testExecuteWithRetryNonSocketException()
    {
        $redis = $this->getMockBuilder(Connection::class)
            ->setMethods(['getIsActive', '__call'])
            ->getMock();
        $redis->expects($this->once())
            ->method('getIsActive')
            ->willReturn(true);
        $redis->expects($this->once())
            ->method('__call')
            ->with('eval', $this->anything())
            ->willThrowException(new \RuntimeException('Lua script error'));

        $semaphore = $this->createTestSemaphore($redis, 'test:key', 10);
        
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Redis semaphore operation failed: Lua script error');
        $this->invokeMethod($semaphore, 'executeWithRetry', [
            function () use ($redis) {
                return $redis->eval('return "success"', 0, [], []);
            }
        ]);
    }

    public function testEnsureConnectionOpenThrowsException()
    {
        $redis = $this->createMock(Connection::class);
        $redis->expects($this->once())
            ->method('getIsActive')
            ->willReturn(false);
        $redis->expects($this->once())
            ->method('open')
            ->willThrowException(new SocketException('Connection failed'));

        $semaphore = $this->createTestSemaphore($redis, 'test:key', 10);
        
        $this->expectException(SocketException::class);
        $this->expectExceptionMessage('Connection failed');
        $this->invokeMethod($semaphore, 'ensureConnection', []);
    }

    public function testExecuteWithRetryCloseThrowsException()
    {
        $redis = $this->getMockBuilder(Connection::class)
            ->setMethods(['getIsActive', 'open', 'close', '__call'])
            ->getMock();
        $redis->expects($this->exactly(2))
            ->method('getIsActive')
            ->willReturn(false);
        $redis->expects($this->exactly(2))
            ->method('open');
        $redis->expects($this->once())
            ->method('close')
            ->willThrowException(new \Exception('Close failed'));
        $redis->expects($this->exactly(2))
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new SocketException('Connection failed')),
                'success'
            );

        $semaphore = $this->createTestSemaphore($redis, 'test:key', 10);
        // close() 抛出异常应该被忽略，重试应该继续
        $result = $this->invokeMethod($semaphore, 'executeWithRetry', [
            function () use ($redis) {
                return $redis->eval('return "success"', 0, [], []);
            }
        ]);

        $this->assertEquals('success', $result);
    }

    public function testExecuteWithRetryWithRetryInterval()
    {
        $redis = $this->getMockBuilder(Connection::class)
            ->setMethods(['getIsActive', 'open', '__call'])
            ->getMock();
        $redis->expects($this->exactly(2))
            ->method('getIsActive')
            ->willReturnOnConsecutiveCalls(false, true);
        $redis->expects($this->once())
            ->method('open');
        $redis->expects($this->exactly(2))
            ->method('__call')
            ->with('eval', $this->anything())
            ->willReturnOnConsecutiveCalls(
                $this->throwException(new SocketException('Connection failed')),
                'success'
            );

        // 创建一个带有 retryInterval 的 semaphore
        $logger = $this->createSilentLogger();
        $semaphore = $this->getMockForAbstractClass(
            Semaphore::class,
            [$redis, 'test:key', 10, $logger, 600, 0]
        );
        $semaphore->expects($this->any())
            ->method('acquire')
            ->willReturn(true);
        $semaphore->expects($this->any())
            ->method('release');
        $semaphore->expects($this->any())
            ->method('heartbeat');

        // 设置 retryInterval（微秒）
        $this->setInaccessibleProperty($semaphore, 'retryInterval', 1000); // 1ms

        $startTime = microtime(true);
        $result = $this->invokeMethod($semaphore, 'executeWithRetry', [
            function () use ($redis) {
                return $redis->eval('return "success"', 0, [], []);
            }
        ]);
        $endTime = microtime(true);

        $this->assertEquals('success', $result);
        // 验证确实等待了（至少 1ms）
        $this->assertGreaterThanOrEqual(0.001, $endTime - $startTime);
    }
}

