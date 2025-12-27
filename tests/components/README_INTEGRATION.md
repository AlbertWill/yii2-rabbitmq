# RabbitMQ 和 Redis 集成测试说明

本文件包含 `yii2-rabbitmq` 扩展的集成测试。这些测试需要真实的 RabbitMQ 和 Redis 服务器运行。

## 目录

- [Redis Semaphore 集成测试](#redis-semaphore-集成测试)
- [RabbitMQ 集成测试](#rabbitmq-集成测试)

---

## Redis Semaphore 集成测试

### 配置

请确保您的环境中设置了以下环境变量，以便测试能够连接到 Redis：

- `REDIS_HOST`: Redis 服务器的主机名或 IP 地址 (例如: `localhost`, `127.0.0.1`)
- `REDIS_PORT`: Redis 服务器的端口 (例如: `6379`)
- `REDIS_DATABASE`: Redis 数据库索引 (例如: `0`)
- `REDIS_PASSWORD`: Redis 密码 (如果 Redis 需要密码认证)

### 示例 (Bash)

```bash
export REDIS_HOST=127.0.0.1
export REDIS_PORT=6379
export REDIS_DATABASE=15 # 建议使用一个独立的数据库进行测试
# export REDIS_PASSWORD=your_redis_password # 如果有密码
```

### 运行测试

在设置好环境变量后，您可以通过以下命令运行集成测试：

```bash
cd /Users/jacky/codes/github/yii2-dev/vendor/albertwill/yii2-rabbitmq
php phpunit.phar tests/components/semaphore/IntegrationSemaphoreTest.php
```

或者运行所有测试：

```bash
cd /Users/jacky/codes/github/yii2-dev/vendor/albertwill/yii2-rabbitmq
php phpunit.phar tests/
```

**注意**:
- 集成测试会连接到真实的 Redis 服务器，并可能对指定的数据库进行数据操作（例如 `DEL` 命令清理测试键）。请确保您使用的数据库是用于测试的，并且不包含重要数据。
- 测试完成后，会自动清理所有以 `test:semaphore:` 开头的键。

---

## RabbitMQ 集成测试

### 配置

请确保您的环境中设置了以下环境变量，以便测试能够连接到 RabbitMQ 和 Redis：

#### RabbitMQ 环境变量

- `RABBITMQ_HOST`: RabbitMQ 服务器的主机名或 IP 地址 (默认: `localhost`)
- `RABBITMQ_PORT`: RabbitMQ 服务器的端口 (默认: `5672`)
- `RABBITMQ_USER`: RabbitMQ 用户名 (默认: `guest`)
- `RABBITMQ_PASSWORD`: RabbitMQ 密码 (默认: `guest`)
- `RABBITMQ_VHOST`: RabbitMQ 虚拟主机 (默认: `/`)

#### Redis 环境变量（用于 semaphore）

- `REDIS_HOST`: Redis 服务器的主机名或 IP 地址 (默认: `localhost`)
- `REDIS_PORT`: Redis 服务器的端口 (默认: `6379`)
- `REDIS_DATABASE`: Redis 数据库索引 (默认: `0`)
- `REDIS_PASSWORD`: Redis 密码 (可选)

### 示例 (Bash)

```bash
# RabbitMQ 配置
export RABBITMQ_HOST=localhost
export RABBITMQ_PORT=5672
export RABBITMQ_USER=guest
export RABBITMQ_PASSWORD=guest
export RABBITMQ_VHOST=/

# Redis 配置（用于 semaphore）
export REDIS_HOST=127.0.0.1
export REDIS_PORT=6379
export REDIS_DATABASE=15
# export REDIS_PASSWORD=your_redis_password # 如果有密码
```

### 运行测试

在设置好环境变量后，您可以通过以下命令运行 RabbitMQ 集成测试：

```bash
cd /Users/jacky/codes/github/yii2-dev/vendor/albertwill/yii2-rabbitmq
php phpunit.phar tests/components/IntegrationRabbitMQTest.php
```

或者运行所有测试：

```bash
cd /Users/jacky/codes/github/yii2-dev/vendor/albertwill/yii2-rabbitmq
php phpunit.phar tests/
```

### 测试内容

RabbitMQ 集成测试包括以下测试用例：

1. **testProducerSendMessage** - 测试 Producer 发送消息到 RabbitMQ
2. **testConsumerReceiveMessage** - 测试 Consumer 从 RabbitMQ 接收消息
3. **testConsumerWithSemaphore** - 测试 Consumer 与 semaphore 的集成
4. **testFullMessageFlow** - 测试完整的消息发送和接收流程

### 注意事项

**重要**:
- 集成测试会连接到真实的 RabbitMQ 和 Redis 服务器
- 测试会创建以下测试资源：
  - 交换器: `test:integration:exchange`
  - 队列: `test:integration:queue`
  - 生产者: `test:integration:producer`
- 测试完成后，会自动清理创建的测试资源（队列和交换器）
- 请确保您使用的 RabbitMQ 服务器是用于测试的，并且不包含重要数据
- 如果 RabbitMQ 或 Redis 不可用，相关测试会自动跳过

### 同时运行所有集成测试

要同时运行 Redis 和 RabbitMQ 的集成测试，请设置所有必需的环境变量：

```bash
# Redis 配置
export REDIS_HOST=127.0.0.1
export REDIS_PORT=6379
export REDIS_DATABASE=15

# RabbitMQ 配置
export RABBITMQ_HOST=localhost
export RABBITMQ_PORT=5672
export RABBITMQ_USER=guest
export RABBITMQ_PASSWORD=guest
export RABBITMQ_VHOST=/

# 运行所有测试
cd /Users/jacky/codes/github/yii2-dev/vendor/albertwill/yii2-rabbitmq
php phpunit.phar tests/
```

