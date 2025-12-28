# 集成测试配置文件说明

## 配置文件使用

集成测试支持从配置文件读取 RabbitMQ 和 Redis 连接信息，避免每次运行测试时都需要设置环境变量。

### 配置步骤

1. **复制配置文件模板**：
   ```bash
   cp tests/config.local.php.example tests/config.local.php
   ```

2. **编辑配置文件**：
   编辑 `tests/config.local.php`，填入实际的连接信息：
   ```php
   <?php
   return [
       'rabbitmq' => [
           'host' => '10.20.9.19',
           'port' => 5677,
           'user' => 'your_username',
           'password' => 'your_password',
           'vhost' => '/your_vhost',
       ],
       'redis' => [
           'host' => '127.0.0.1',
           'port' => 6379,
           'database' => 0,
           'password' => 'your_redis_password', // 如果没有密码，设置为 null
       ],
   ];
   ```

3. **运行测试**：
   ```bash
   php vendor/bin/phpunit tests/
   ```

### 配置优先级

测试代码会按以下优先级读取配置：

1. **配置文件** (`tests/config.local.php`) - 最高优先级
2. **环境变量** (`RABBITMQ_*`, `REDIS_*`)
3. **默认值** (localhost, 6379, guest/guest 等)

### 安全说明

- `tests/config.local.php` 文件已被添加到 `.gitignore`，不会提交到版本库
- 请妥善保管配置文件，不要将包含敏感信息的配置文件提交到版本库
- 如果使用环境变量方式，确保不会在日志或错误信息中泄露密码

### 环境变量方式（备选）

如果不想使用配置文件，也可以通过环境变量传递配置：

```bash
RABBITMQ_HOST=10.20.9.19 \
RABBITMQ_PORT=5677 \
RABBITMQ_USER=Jacky \
RABBITMQ_PASSWORD=wysh2fBDKCnyxv8 \
RABBITMQ_VHOST=/jacky_test1 \
REDIS_HOST=127.0.0.1 \
REDIS_PORT=6379 \
REDIS_DATABASE=0 \
REDIS_PASSWORD=jacky123 \
php vendor/bin/phpunit tests/
```

### 测试跳过

如果 RabbitMQ 或 Redis 服务不可用，相关的集成测试会自动跳过，不会导致测试失败。

