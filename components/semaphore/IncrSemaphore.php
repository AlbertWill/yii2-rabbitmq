<?php

namespace mikemadisonweb\rabbitmq\components\semaphore;


/**
 * 基于全局 INCR/DECR 方式控制的全局信号量
 * @author jacky 2025-12-25 20:34
 */
class IncrSemaphore extends Semaphore
{
    public function acquire(): bool
    {
        $lua = <<<LUA
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])

local current = tonumber(redis.call("GET", key) or "0")
if current >= limit then
    return 0
end

current = redis.call("INCR", key)
-- 每次成功获取后都续期 TTL，确保活跃的信号量不会过期
redis.call("EXPIRE", key, ttl)

return 1
LUA;

        return $this->evalLua($lua, [$this->key], [$this->limit, $this->ttl]) === 1;
    }

    public function release(): void
    {
        $lua = <<<LUA
local key = KEYS[1]
local ttl = tonumber(ARGV[1])

if redis.call("EXISTS", key) == 0 then
    return 0
end

local current = tonumber(redis.call("GET", key) or "0")
if current <= 0 then
    return 0
end

local result = redis.call("DECR", key)

if result > 0 then
    redis.call("EXPIRE", key, ttl)
end

return result
LUA;

        $this->evalLua($lua, [$this->key], [$this->ttl]);
    }

    public function heartbeat(): void
    {
        $lua = <<<LUA
local key = KEYS[1]
local ttl = tonumber(ARGV[1])

-- 检查 key 是否存在且值 > 0，存在则续期 TTL
if redis.call("EXISTS", key) == 1 then
    local current = tonumber(redis.call("GET", key) or "0")
    if current > 0 then
        return redis.call("EXPIRE", key, ttl)
    end
end
return 0
LUA;

        $this->evalLua($lua, [$this->key], [$this->ttl]);
    }
}
