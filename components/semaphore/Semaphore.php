<?php

namespace mikemadisonweb\rabbitmq\components\semaphore;

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
     * 构造函数
     * @param Connection $redis Redis 连接实例（yii\redis\Connection）
     * @param string $key Redis key
     * @param int $limit 并发限制数
     * @param int $ttl 过期时间（秒）
     * @param int $acquireSleep 获取信号量失败时的等待间隔时间（秒），每次重试前等待的秒数，默认 0 表示不等待直接返回
     */
    public function __construct(Connection $redis, string $key, int $limit, int $ttl = 600, int $acquireSleep = 0)
    {
        $this->redis = $redis;
        $this->key = $key;
        $this->limit = $limit;
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
        // 如果未设置等待间隔时间，直接尝试一次获取
        if ($this->acquireSleep <= 0) {
            return $this->acquire();
        }

        // 循环尝试获取信号量，直到成功
        while (true) {
            // 尝试获取信号量
            if ($this->acquire()) {
                return true;
            }

            // 获取失败，等待 acquireSleep 秒后重试
            sleep($this->acquireSleep);
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

        return $this->redis->eval($lua, count($keys), $keys, $args);
    }
}
