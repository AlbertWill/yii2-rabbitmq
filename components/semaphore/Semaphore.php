<?php

namespace mikemadisonweb\rabbitmq\components\semaphore;

use mikemadisonweb\rabbitmq\components\Logger;
use yii\redis\Connection;

/**
 * 信号量抽象基类
 * 提供共同的属性和构造函数逻辑
 */
abstract class Semaphore
{
    /**
     * @var string Redis key
     */
    protected $key;

    /**
     * @var int 并发限制数
     */
    protected $limit;

    /**
     * @var int 过期时间（秒）
     */
    protected $ttl;

    /**
     * @var Connection Redis 实例，需要实现 eval(string $script, int $numKeys, array $keys, array $args) 方法
     */
    protected $redis;

    /**
     * @var int 获取信号量失败时的等待间隔时间（秒），每次重试前等待的秒数
     */
    protected $acquireSleep;

    /**
     * @var int Redis 连接失败时的最大重试次数
     */
    protected $maxRetries = 3;

    /**
     * @var int Redis 连接失败时重试间隔（微秒），默认 100ms
     */
    protected $retryInterval = 100000;

    /**
     * @var Logger Logger 实例
     */
    protected $logger;

    /**
     * 构造函数
     * @param Connection $redis Redis 连接实例（yii\redis\Connection）
     * @param string $key Redis key
     * @param int $limit 并发限制数
     * @param Logger $logger Logger 实例
     * @param int $ttl 过期时间（秒）
     * @param int $acquireSleep 获取信号量失败时的等待间隔时间（秒），每次重试前等待的秒数，默认 0 表示不等待直接返回
     */
    public function __construct(Connection $redis, string $key, int $limit, Logger $logger, int $ttl = 600, int $acquireSleep = 60)
    {
        $this->redis = $redis;
        $this->key = $key;
        $this->limit = $limit;
        $this->logger = $logger;
        $this->ttl = $ttl;
        $this->acquireSleep = $acquireSleep;
    }

    /**
     * 尝试获取信号量，如果获取不到则等待指定时间后重试
     * 每次获取失败后会等待 acquireSleep 秒后再次尝试，直到成功获取到信号量
     *
     * @return bool 成功获取到信号量返回 true
     */
    public function acquire_wait(): bool
    {
        $this->logger->logDebug("[Semaphore] acquire_wait() 开始，key: {$this->key}, limit: {$this->limit}, acquireSleep: {$this->acquireSleep}");

        // 如果 acquireSleep <= 0（理论上不应该发生，配置验证已确保 >= 1），直接尝试一次获取
        if ($this->acquireSleep <= 0) {
            $result = $this->acquire();
            return $result;
        }

        // 循环尝试获取信号量，直到成功
        $attemptCount = 0;
        while (true) {
            $attemptCount++;

            // 尝试获取信号量
            $acquireStart = microtime(true);
            $result = $this->acquire();
            $acquireEnd = microtime(true);
            $acquireTime = round(($acquireEnd - $acquireStart) * 1000, 2);
            if ($result) {
                $this->logger->logDebug("[Semaphore] 第 {$attemptCount} 次获取信号量成功, 耗时: {$acquireTime}ms, key: {$this->key}");
                return true;
            }

            // 获取失败，随机浮动（±20%）以分散重试请求，避免所有进程同时重试造成 Redis 压力峰值
            // 例如：acquireSleep=60 秒时，实际等待时间为 48-72 秒之间随机
            $jitterPercent = 0.2; // 20% 的随机浮动
            $jitterRange = (int)round($this->acquireSleep * $jitterPercent);
            $sleepTimeWithJitter = $this->acquireSleep + mt_rand(-$jitterRange, $jitterRange);
            $sleepTimeWithJitter = max(1, $sleepTimeWithJitter); // 至少等待 1 秒

            $this->logger->logDebug("[Semaphore] 第 {$attemptCount} 次获取信号量失败, 等待{$sleepTimeWithJitter}秒（基础: {$this->acquireSleep}秒, 浮动范围: ±{$jitterRange}秒）, key: {$this->key}");

            // 如果返回值 > 0，说明被信号中断，应该处理信号并返回 false 让上层判断
            $sleepStart = microtime(true);
            $remainingSeconds = sleep($sleepTimeWithJitter);
            $sleepEnd = microtime(true);
            $actualSleepTimeElapsed = round($sleepEnd - $sleepStart, 2);

            // 如果 sleep() 被信号中断（返回值 > 0），返回 false 让上层处理信号
            if ($remainingSeconds > 0) {
                $this->logger->logDebug("[Semaphore] sleep 被信号中断, 剩余时间: {$remainingSeconds}秒, 实际等待: {$actualSleepTimeElapsed}秒, key: {$this->key}");

                // 返回 false，让上层（Consumer）处理挂起的信号并根据 forceStop 标志判断是否退出
                return false;
            }
        }
    }

    /**
     * 尝试获取并发名额
     * @return bool
     */
    abstract public function acquire(): bool;

    /**
     * 释放名额
     */
    abstract public function release(): void;

    /**
     * 心跳刷新 TTL（可选）
     */
    abstract public function heartbeat(): void;

    /**
     * 执行 Lua 脚本的辅助方法
     * 自动处理 Redis 连接断开和重试
     *
     * @param string $lua Lua 脚本
     * @param array $keys 键的数组
     * @param array $args 参数的数组
     * @return mixed
     * @throws \Exception 当 Redis 连接失败或脚本执行失败时抛出异常
     */
    protected function evalLua(string $lua, array $keys, array $args = [])
    {
        if (empty($lua)) {
            throw new \InvalidArgumentException('Lua script cannot be empty');
        }

        return $this->executeWithRetry(function () use ($lua, $keys, $args) {
            // Yii2 Redis eval 方法签名: eval($script, $numkeys, ...$keys, ...$args)
            // 需要将所有参数展开，而不是作为数组传递
            $params = array_merge([$lua, count($keys)], $keys, $args);
            $result = $this->redis->__call('eval', $params);
            // Lua 脚本返回的整数在 Redis 中可能是字符串，需要转换为整数
            if (is_string($result) && is_numeric($result)) {
                return (int)$result;
            }
            return $result;
        });
    }

    /**
     * 确保 Redis 连接是活跃的
     * 如果连接不存在或已断开，尝试重新连接
     *
     * @throws \yii\db\Exception 当连接失败时抛出异常
     */
    protected function ensureConnection(): void
    {
        if (!$this->redis->getIsActive()) {
            $this->redis->open();
        }
    }

    /**
     * 执行带重试的 Redis 操作
     * 当遇到连接异常时自动重试，适用于长时间运行的消费者进程
     *
     * @param callable $operation 要执行的操作（闭包函数）
     * @return mixed 操作的返回值
     * @throws \RuntimeException 当所有重试都失败时抛出异常
     */
    protected function executeWithRetry(callable $operation)
    {
        $lastException = null;

        for ($attempt = 0; $attempt < $this->maxRetries; $attempt++) {
            try {
                // 确保连接是活跃的
                $this->ensureConnection();

                // 执行操作
                return $operation();
            } catch (\yii\redis\SocketException $e) {
                // Redis 连接异常，尝试重连
                $lastException = $e;

                if ($attempt < $this->maxRetries - 1) {
                    // 关闭旧连接并等待后重试
                    try {
                        $this->redis->close();
                    } catch (\Exception $closeEx) {
                        // 忽略关闭时的异常
                    }

                    // 等待后重试
                    if ($this->retryInterval > 0) {
                        usleep($this->retryInterval);
                    }

                    // 继续下一次重试
                    continue;
                }
                // 最后一次重试失败，抛出异常
                break;
            } catch (\Exception $e) {
                // 其他异常（如 Lua 脚本错误、Redis 命令错误等）直接抛出，不重试
                throw new \RuntimeException(
                    "Redis semaphore operation failed: " . $e->getMessage(),
                    $e->getCode(),
                    $e
                );
            }
        }

        // 所有重试都失败
        $message = "Redis semaphore operation failed after {$this->maxRetries} attempts: " .
                   ($lastException ? $lastException->getMessage() : 'Unknown error');
        $code = $lastException ? (int)$lastException->getCode() : 0;
        throw new \RuntimeException($message, $code, $lastException);
    }
}
