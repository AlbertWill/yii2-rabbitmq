RabbitMQ Extension for Yii2
==================

**高级用法**

为了防止与 RabbitMQ 交换消息时丢失消息，建议使用扩展设置来配置生产者和监听器（工作进程）。

**配置示例：**

```php
<?php

use app\components\TestConsumer;
use mikemadisonweb\rabbitmq\Configuration;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Connection\AMQPSSLConnection;

return [
    'class' => Configuration::class,
    'connections' => [
        [
            'type' => $_ENV['RABBITMQ_SSL'] ? AMQPSSLConnection::class : AMQPLazyConnection::class,
            'host' => $_ENV['RABBITMQ_HOST'],
            'port' => $_ENV['RABBITMQ_PORT'],
            'user' => $_ENV['RABBITMQ_USER'],
            'password' => $_ENV['RABBITMQ_PASSWD'],
            'vhost' => $_ENV['RABBITMQ_VHOST'],
            'ssl_context' => $_ENV['RABBITMQ_SSL'] ? [
                'capath' => null,
                'cafile' => null,
                'verify_peer' => false,
            ] : null
        ],
    ],
    'exchanges' => [
        [
            'name' => 'test_exchange',
            'type' => 'direct'
        ],
    ],
    'queues' => [
        [
            'name' => 'test_queue',
        ],
    ],
    'producers' => [
        [
            'name' => 'test_producer',
        ],
    ],
    'bindings' => [
        [
            'queue' => 'test_queue',
            'exchange' => 'test_exchange',
        ],
    ],
    'consumers' => [
        [
            'name' => 'test_consumer',
            'callbacks' => [
                'test_queue' => TestConsumer::class
            ],
            'systemd' => [
                'memory_limit' => 8, // mb
                'workers' => 3
            ],
        ],
    ],
];
```

--------------------

**生产者配置** 主要是将未发送的消息保存到 `rabbit_publish_error` 表中（类 `\mikemadisonweb\rabbitmq\models\RabbitPublishError`），然后通过定时任务（如 cron）发送。

* 在控制台应用的配置文件中，在 controllerMap 部分添加组件迁移的命名空间

```
...
'controllerMap' => [
        'migrate' => [
            'class' => 'yii\console\controllers\MigrateController',
            'migrationNamespaces' => [
                'mikemadisonweb\rabbitmq\migrations'
            ],
        ],
    ],
...
```

执行 `php yii migrate`

* 在调用生产者时捕获异常，并将消息写入数据库，示例：

```php
public function actionPublish()
{
    $producer = \Yii::$app->rabbitmq->getProducer('test_producer');
    $data = [
        'counter' => 1,
        'msg' => 'I\'am test publish'
    ];
    while (true) {
        sleep(1);
        try {
            $producer->publish(json_encode($data), 'test_exchange');
            $data['counter']++;
        } catch (\Exception $e) {
            $model_error = new RabbitPublishError();
            $model_error->exchangeName = 'test_exchange';
            $model_error->producerName = 'test_producer';
            $model_error->msgBody = json_encode($data);
            $model_error->errorMsg = $e->getMessage();
            $model_error->saveItem();
        }
    }
}
```

* 重新发送已保存消息的示例

```php
public function actionRePublish()
{
    $republish = new RabbitPublishError();
    $republish->rePublish();
}
```

如果重新发送消息成功，则删除记录，否则 counter 字段增加 1。

--------------

**对于工作进程的高级配置**，需要使用 systemd 将它们作为守护进程启动。   
通过 systemd 我们可以解决两个主要问题：

1. 连接断开时自动重启工作进程

2. 达到内存限制时自动重启工作进程

此外，使用 systemd 我们可以为同一个队列启动多个工作进程实例

* 在 rabbitmq 配置中，在 `consumers` 部分添加 systemd 的额外设置：为队列 `test_queue` 启动三个工作进程 `test_consumer`，每个进程的内存限制为 8 MB。

```php
'consumers' => [
    [
        'name' => 'test_consumer',
        'callbacks' => [
            'test_queue' => TestConsumer::class
        ],
        'systemd' => [
            'memory_limit' => 8, // mb
            'workers' => 3
        ],
    ],
],
```  

* 建议使用辅助类 `\mikemadisonweb\rabbitmq\helpers\CreateUnitHelper` 自动生成 systemd 单元文件

在声明辅助类时需要定义以下字段：

```php
/** @var string 创建单元文件的文件夹，必须可写 */
public $units_dir;

/** @var string 运行单元文件的用户名 */
public $user;

/** @var string 运行单元文件的组名 */
public $group;

/** @var string 包含 yii 可执行文件的目录 */
public $work_dir;
```

辅助类中还有一个 `example` 字段，其中存储了生成单元文件的模板。建议研究它，必要时可以重新声明。特别注意 `[Unit]` 部分

```php
public $example = '[Unit]
Description=%description%
After=syslog.target
After=network.target
After=postgresql.service
Requires=postgresql.service

[Service]
Type=simple
WorkingDirectory=%work_dir%

User=%user%
Group=%group%

ExecStart=php %yii_path% rabbitmq/consume %name_consumer% %memory_limit%
ExecReload=php %yii_path% rabbitmq/restart-consume %name_consumer% %memory_limit%
TimeoutSec=3
Restart=always

[Install]
WantedBy=multi-user.target';
```

使用辅助类的控制器示例

```php
<?php

namespace app\commands;

use mikemadisonweb\rabbitmq\helpers\CreateUnitHelper;
use yii\console\Controller;
use Yii;

class CreateUnitsController extends Controller
{
    public function actionIndex()
    {
        $helper = new CreateUnitHelper(
            [
                'units_dir' => Yii::getAlias('@runtime/units'),
                'work_dir' => Yii::getAlias('@app'),
                'user' => 'vagrant',
                'group' => 'vagrant',
            ]
        );

        $helper->create();
    }
}
```

不要忘记运行生成单元文件的命令：`php yii create-units`

* 生成单元文件后，在单元文件文件夹中还会生成一个 bash 脚本 exec.sh。运行时可以传入以下命令：`copy | start | restart | status | delete`。此脚本使用掩码处理所有生成的单元文件。

首次生成单元文件后，只需运行命令 `sh exec.sh copy`

**因此，要使用高级工作进程功能**，需要执行三个步骤：

1. 在 RabbitMQ 配置中声明 systemd 参数

2. 生成 systemd 单元文件

3. 将工作进程作为 systemd 管理的守护进程启动

**Enjoy!**
