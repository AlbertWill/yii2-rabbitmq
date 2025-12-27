# 独立运行测试指南

## 概述

本扩展的测试已经支持独立运行，无需依赖外层项目的 `vendor` 目录。测试配置会自动检测并使用可用的依赖：

1. **优先使用本地 vendor**：如果扩展目录下有独立的 `vendor` 目录（非符号链接），则使用本地依赖
2. **回退到外层 vendor**：如果本地 `vendor` 不存在，则自动使用外层项目的 `vendor` 目录

## 运行方式

### 方式 1：使用本地 vendor（推荐，完全独立）

如果扩展目录下有独立的 `vendor` 目录：

```bash
# 进入扩展目录
cd /path/to/yii2-rabbitmq

# 运行测试
./vendor/bin/phpunit tests/ --configuration phpunit.xml.dist --no-coverage

# 或使用 PHAR 版本
php phpunit.phar tests/ --configuration phpunit.xml.dist --no-coverage
```

### 方式 2：使用外层项目的 vendor（共享依赖）

如果扩展目录下的 `vendor` 是符号链接或不存在，测试会自动使用外层项目的 `vendor`：

```bash
# 进入扩展目录
cd /path/to/yii2-rabbitmq

# 运行测试（会自动使用外层 vendor）
php phpunit.phar tests/ --configuration phpunit.xml.dist --no-coverage
```

## 安装独立依赖

如果需要完全独立运行测试，可以在扩展目录下安装依赖：

```bash
# 进入扩展目录
cd /path/to/yii2-rabbitmq

# 如果 vendor 是符号链接，先移除
if [ -L vendor ]; then
    rm vendor
fi

# 安装依赖
composer install --no-interaction

# 运行测试
./vendor/bin/phpunit tests/ --configuration phpunit.xml.dist --no-coverage
```

## 技术实现

测试的自动检测逻辑在以下文件中实现：

- **`tests/bootstrap.php`**：自动检测并加载 `vendor/autoload.php` 和 `Yii.php`
- **`tests/TestCase.php`**：`getVendorPath()` 方法自动检测 vendor 路径

实现逻辑：
1. 优先查找本地 `vendor` 目录（不是符号链接）
2. 如果本地不存在，回退到外层项目的 `vendor` 目录
3. 如果都不存在，显示错误提示

## 注意事项

1. **磁盘空间**：独立安装会占用更多磁盘空间（约 50-100MB）
2. **依赖更新**：需要定期运行 `composer update` 更新依赖
3. **符号链接**：如果 `vendor` 是符号链接，测试会自动使用外层项目的依赖，无需额外操作

## 快速开始

```bash
# 进入扩展目录
cd /path/to/yii2-rabbitmq

# 直接运行测试（会自动检测并使用可用的 vendor）
php phpunit.phar tests/ --configuration phpunit.xml.dist --no-coverage
```

测试会自动处理依赖路径，无需手动配置。

