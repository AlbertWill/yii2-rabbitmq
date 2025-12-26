<?php

namespace mikemadisonweb\rabbitmq\components\semaphore;

use yii\redis\Connection;

/**
 * 基于 Set/Hash token 方式控制的全局信号量
 * @author jacky 2025-12-25 20:34
 */
class HashSemaphore extends Semaphore
{
    /**
     * @var string Token 标识
     */
    private $token;

    public function __construct(Connection $redis, string $key, int $limit, int $ttl = 600)
    {
        parent::__construct($redis, $key, $limit, $ttl);
        $this->token = self::genToken();
    }

    public function acquire(): bool
    {
        $lua = <<<LUA
local key = KEYS[1]
local token = ARGV[1]
local limit = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

-- 添加 token，SADD 返回 1 表示新添加，0 表示已存在（理论上不应该发生）
local added = redis.call("SADD", key, token)
local count = redis.call("SCARD", key)

-- 如果超过限制，移除刚添加的 token
if count > limit then
    redis.call("SREM", key, token)
    return 0
end

-- 成功获取后续期 TTL，确保活跃的信号量不会过期
redis.call("EXPIRE", key, ttl)
return 1
LUA;

        return $this->evalLua($lua, [$this->key], [$this->token, $this->limit, $this->ttl]) === 1;
    }

    public function release(): void
    {
        $lua = <<<LUA
local key = KEYS[1]
local token = ARGV[1]
local ttl = tonumber(ARGV[2])

-- 检查 token 是否存在，如果不存在则直接返回（避免无效操作）
if redis.call("SISMEMBER", key, token) == 0 then
    return 0
end

-- 移除 token，SREM 返回 1 表示成功移除，0 表示不存在（理论上不会发生）
redis.call("SREM", key, token)

-- 如果 Set 中还有元素，续期 TTL；如果为空，让 key 自然过期
if redis.call("SCARD", key) > 0 then
    redis.call("EXPIRE", key, ttl)
end

return 1
LUA;

        $this->evalLua($lua, [$this->key], [$this->token, $this->ttl]);
    }

    public function heartbeat(): void
    {
        $lua = <<<LUA
local key = KEYS[1]
local token = ARGV[1]
local ttl = tonumber(ARGV[2])

if redis.call("SISMEMBER", key, token) == 1 then
    return redis.call("EXPIRE", key, ttl)
end
return 0
LUA;

        $this->evalLua($lua, [$this->key], [$this->token, $this->ttl]);
    }

    /**
     * 生成Token
     * 使用加密安全随机数生成器，碰撞概率极低（2^96 种可能）
     * 12 字节的 base64 编码结果固定为 16 字符，无需填充（12 ÷ 3 = 4 组，正好整除）
     * 因此替换 '=' 为空字符串不会影响长度（base64 的 '=' 只作为末尾填充出现）
     */
    public static function genToken(): string
    {
        return str_replace(['+', '/', '='], ['-', '_', ''], base64_encode(random_bytes(12)));
    }

}
