<?php

namespace mikemadisonweb\rabbitmq\components\semaphore;

use Yii;

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
     * 构造函数
     * @param string $key Redis key
     * @param int $limit 并发限制数
     * @param int $ttl 过期时间（秒）
     */
    public function __construct(string $key, int $limit, int $ttl = 600)
    {
        $this->key = $key;
        $this->limit = $limit;
        $this->ttl = $ttl;
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
        
        return Yii::$app->redis->eval($lua, count($keys), $keys, $args);
    }
}
