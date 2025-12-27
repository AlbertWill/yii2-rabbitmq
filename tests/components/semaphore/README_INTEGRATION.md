# Semaphore 集成测试说明

## 概述

`IntegrationSemaphoreTest.php` 使用真实的 Redis 连接测试 semaphore 的并发场景。这些测试验证了 semaphore 在实际环境中的行为。

## 配置要求

### 环境变量

通过环境变量配置 Redis 连接信息：

```bash
# 必需配置
REDIS_HOST=localhost          # Redis 主机地址（默认: localhost）
REDIS_PORT=6379              # Redis 端口（默认: 6379）

# 可选配置
REDIS_DATABASE=0             # Redis 数据库（默认: 0）
REDIS_PASSWORD=your_password # Redis 密码（可选）
```

### 运行测试

#### 方式 1：使用环境变量

```bash
cd /Users/jacky/codes/github/yii2-dev/vendor/albertwill/yii2-rabbitmq

# 设置环境变量并运行测试
REDIS_HOST=localhost REDIS_PORT=6379 php phpunit.phar tests/components/semaphore/IntegrationSemaphoreTest.php --configuration phpunit.xml.dist --no-coverage
```

#### 方式 2：使用 .env 文件（如果支持）

```bash
# 创建 .env 文件
cat > .env <<EOF
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DATABASE=0
REDIS_PASSWORD=
EOF

# 运行测试（需要支持 .env 的加载器）
php phpunit.phar tests/components/semaphore/IntegrationSemaphoreTest.php --configuration phpunit.xml.dist --no-coverage
```

#### 方式 3：导出环境变量

```bash
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_DATABASE=0
# export REDIS_PASSWORD=your_password  # 如果需要密码

php phpunit.phar tests/components/semaphore/IntegrationSemaphoreTest.php --configuration phpunit.xml.dist --no-coverage
```

## 测试用例

### 1. testIncrSemaphoreRealConcurrentAcquire
- **功能**：测试 IncrSemaphore 的真实并发获取
- **场景**：10 个并发请求，limit=5，验证只有 5 个成功

### 2. testHashSemaphoreRealConcurrentAcquire
- **功能**：测试 HashSemaphore 的真实并发获取
- **场景**：10 个并发请求，limit=5，验证只有 5 个成功

### 3. testRealAcquireReleaseAcquireFlow
- **功能**：测试获取-释放-再获取的完整流程
- **场景**：获取 → 释放 → 再次获取

### 4. testRealAcquireUntilLimitThenReleaseAndAcquire
- **功能**：测试达到 limit 后释放，其他实例可以获取
- **场景**：达到 limit → 释放 → 新实例可以获取

### 5. testRealMultipleAcquireReleaseCycles
- **功能**：测试多次获取和释放的循环
- **场景**：执行 10 次获取-释放循环

### 6. testRealHeartbeat
- **功能**：测试 heartbeat 续期功能
- **场景**：获取后检查 TTL，执行 heartbeat，验证 TTL 被刷新

### 7. testRealAcquireWait
- **功能**：测试 acquire_wait 功能
- **场景**：达到 limit 后，使用 acquire_wait 等待获取

### 8. testRealAcquireReleaseWithLimitOne
- **功能**：测试边界情况：limit = 1
- **场景**：limit=1 时的获取和释放

### 9. testRealConcurrentAcquireWithStatistics
- **功能**：测试并发场景并输出统计信息
- **场景**：20 个并发请求，limit=5，输出详细的统计信息

## 注意事项

1. **Redis 可用性**：如果 Redis 不可用，所有测试会自动跳过
2. **数据清理**：测试会自动清理使用的 Redis key（`test:semaphore:*`）
3. **并发模拟**：由于 PHP 单线程特性，测试通过快速顺序执行来模拟并发
4. **真实并发**：如需测试真正的并发场景，需要使用多进程或多线程工具

## 示例输出

```
并发测试统计:
  总请求数: 20
  成功数: 5
  失败数: 15
  总耗时: 0.123 秒
  平均耗时: 6.15 毫秒
  最大耗时: 12.34 毫秒
  最小耗时: 3.21 毫秒
```

## 故障排查

### Redis 连接失败

如果测试被跳过，检查：
1. Redis 服务是否运行：`redis-cli ping`
2. 环境变量是否正确设置
3. 网络连接是否正常
4. 防火墙设置

### 测试数据未清理

如果测试后 Redis 中仍有测试数据：
- 手动清理：`redis-cli KEYS "test:semaphore:*" | xargs redis-cli DEL`

