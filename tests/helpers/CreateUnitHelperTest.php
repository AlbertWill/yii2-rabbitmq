<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\helpers;

use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\helpers\CreateUnitHelper;
use mikemadisonweb\rabbitmq\tests\TestCase;

class CreateUnitHelperTest extends TestCase
{
    /**
     * @var string 临时测试目录
     */
    private $testDir;

    protected function setUp(): void
    {
        parent::setUp();
        // 创建临时测试目录
        $this->testDir = sys_get_temp_dir() . '/rabbitmq_test_' . uniqid();
    }

    protected function tearDown(): void
    {
        // 清理测试目录
        if (is_dir($this->testDir)) {
            $this->removeDirectory($this->testDir);
        }
        parent::tearDown();
    }

    /**
     * 递归删除目录
     */
    private function removeDirectory(string $dir): void
    {
        if (!is_dir($dir)) {
            return;
        }
        $files = array_diff(scandir($dir), ['.', '..']);
        foreach ($files as $file) {
            $path = $dir . '/' . $file;
            is_dir($path) ? $this->removeDirectory($path) : unlink($path);
        }
        rmdir($dir);
    }

    /**
     * 测试创建单个 worker 的服务文件
     */
    public function testCreateSingleWorker()
    {
        $this->mockApplication([
            'id' => 'testapp',
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        ['host' => 'localhost'],
                    ],
                    'queues' => [
                        ['name' => 'test_queue'],
                    ],
                    'consumers' => [
                        [
                            'name' => 'test_consumer',
                            'callbacks' => [
                                'test_queue' => 'TestCallback',
                            ],
                            'systemd' => [
                                'memory_limit' => 0,
                                'workers' => 1,
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        $helper = new CreateUnitHelper([
            'units_dir' => $this->testDir,
            'user' => 'testuser',
            'group' => 'testgroup',
            'work_dir' => '/path/to/work',
        ]);
        $helper->init();
        $helper->create();

        // 验证生成了 1 个服务文件
        $serviceFile = $this->testDir . '/consumer_test_consumer_1.service';
        $this->assertFileExists($serviceFile);

        // 验证服务文件内容
        $content = file_get_contents($serviceFile);
        $this->assertStringContainsString('Description=Consumer test_consumer', $content);
        $this->assertStringContainsString('WorkingDirectory=/path/to/work', $content);
        $this->assertStringContainsString('User=testuser', $content);
        $this->assertStringContainsString('Group=testgroup', $content);
        $this->assertStringContainsString('ExecStart=php /path/to/work/yii rabbitmq/consume test_consumer', $content);
        $this->assertStringNotContainsString('-l', $content); // memory_limit = 0 时不包含 -l 参数

        // 验证生成了 bash 脚本
        $bashFile = $this->testDir . '/exec.sh';
        $this->assertFileExists($bashFile);
        // 验证文件权限（应该是可执行的）
        $perms = fileperms($bashFile);
        $this->assertNotFalse($perms);
        $this->assertTrue(($perms & 0111) !== 0, 'Bash script should be executable');
    }

    /**
     * 测试创建多个 worker 的服务文件
     */
    public function testCreateMultipleWorkers()
    {
        $this->mockApplication([
            'id' => 'testapp',
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        ['host' => 'localhost'],
                    ],
                    'queues' => [
                        ['name' => 'test_queue'],
                    ],
                    'consumers' => [
                        [
                            'name' => 'test_consumer',
                            'callbacks' => [
                                'test_queue' => 'TestCallback',
                            ],
                            'systemd' => [
                                'memory_limit' => 8,
                                'workers' => 3,
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        $helper = new CreateUnitHelper([
            'units_dir' => $this->testDir,
            'user' => 'testuser',
            'group' => 'testgroup',
            'work_dir' => '/path/to/work',
        ]);
        $helper->init();
        $helper->create();

        // 验证生成了 3 个服务文件
        for ($i = 1; $i <= 3; $i++) {
            $serviceFile = $this->testDir . '/consumer_test_consumer_' . $i . '.service';
            $this->assertFileExists($serviceFile, "Service file $i should exist");

            // 验证每个服务文件都包含内存限制参数
            $content = file_get_contents($serviceFile);
            $this->assertStringContainsString('ExecStart=php /path/to/work/yii rabbitmq/consume test_consumer -l 8', $content);
        }

        // 验证没有生成第 4 个文件
        $nonExistentFile = $this->testDir . '/consumer_test_consumer_4.service';
        $this->assertFileDoesNotExist($nonExistentFile);
    }

    /**
     * 测试内存限制为 0 时不添加 -l 参数
     */
    public function testMemoryLimitZero()
    {
        $this->mockApplication([
            'id' => 'testapp',
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        ['host' => 'localhost'],
                    ],
                    'queues' => [
                        ['name' => 'test_queue'],
                    ],
                    'consumers' => [
                        [
                            'name' => 'test_consumer',
                            'callbacks' => [
                                'test_queue' => 'TestCallback',
                            ],
                            'systemd' => [
                                'memory_limit' => 0,
                                'workers' => 1,
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        $helper = new CreateUnitHelper([
            'units_dir' => $this->testDir,
            'user' => 'testuser',
            'group' => 'testgroup',
            'work_dir' => '/path/to/work',
        ]);
        $helper->init();
        $helper->create();

        $serviceFile = $this->testDir . '/consumer_test_consumer_1.service';
        $content = file_get_contents($serviceFile);
        $this->assertStringContainsString('ExecStart=php /path/to/work/yii rabbitmq/consume test_consumer', $content);
        $this->assertStringNotContainsString('-l', $content);
    }

    /**
     * 测试内存限制非 0 时添加 -l 参数
     */
    public function testMemoryLimitNonZero()
    {
        $this->mockApplication([
            'id' => 'testapp',
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        ['host' => 'localhost'],
                    ],
                    'queues' => [
                        ['name' => 'test_queue'],
                    ],
                    'consumers' => [
                        [
                            'name' => 'test_consumer',
                            'callbacks' => [
                                'test_queue' => 'TestCallback',
                            ],
                            'systemd' => [
                                'memory_limit' => 128,
                                'workers' => 1,
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        $helper = new CreateUnitHelper([
            'units_dir' => $this->testDir,
            'user' => 'testuser',
            'group' => 'testgroup',
            'work_dir' => '/path/to/work',
        ]);
        $helper->init();
        $helper->create();

        $serviceFile = $this->testDir . '/consumer_test_consumer_1.service';
        $content = file_get_contents($serviceFile);
        $this->assertStringContainsString('ExecStart=php /path/to/work/yii rabbitmq/consume test_consumer -l 128', $content);
    }

    /**
     * 测试多个消费者
     */
    public function testMultipleConsumers()
    {
        $this->mockApplication([
            'id' => 'testapp',
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        ['host' => 'localhost'],
                    ],
                    'queues' => [
                        ['name' => 'queue1'],
                        ['name' => 'queue2'],
                    ],
                    'consumers' => [
                        [
                            'name' => 'consumer1',
                            'callbacks' => [
                                'queue1' => 'Callback1',
                            ],
                            'systemd' => [
                                'memory_limit' => 8,
                                'workers' => 2,
                            ],
                        ],
                        [
                            'name' => 'consumer2',
                            'callbacks' => [
                                'queue2' => 'Callback2',
                            ],
                            'systemd' => [
                                'memory_limit' => 16,
                                'workers' => 1,
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        $helper = new CreateUnitHelper([
            'units_dir' => $this->testDir,
            'user' => 'testuser',
            'group' => 'testgroup',
            'work_dir' => '/path/to/work',
        ]);
        $helper->init();
        $helper->create();

        // 验证 consumer1 生成了 2 个服务文件
        $this->assertFileExists($this->testDir . '/consumer_consumer1_1.service');
        $this->assertFileExists($this->testDir . '/consumer_consumer1_2.service');

        // 验证 consumer2 生成了 1 个服务文件
        $this->assertFileExists($this->testDir . '/consumer_consumer2_1.service');

        // 验证 consumer1 的服务文件包含正确的内存限制
        $content1 = file_get_contents($this->testDir . '/consumer_consumer1_1.service');
        $this->assertStringContainsString('ExecStart=php /path/to/work/yii rabbitmq/consume consumer1 -l 8', $content1);

        // 验证 consumer2 的服务文件包含正确的内存限制
        $content2 = file_get_contents($this->testDir . '/consumer_consumer2_1.service');
        $this->assertStringContainsString('ExecStart=php /path/to/work/yii rabbitmq/consume consumer2 -l 16', $content2);
    }

    /**
     * 测试自动创建目录
     */
    public function testAutoCreateDirectory()
    {
        // 确保父目录存在
        if (!is_dir($this->testDir)) {
            mkdir($this->testDir, 0755, true);
        }
        $nonExistentDir = $this->testDir . '/subdir';
        if (is_dir($nonExistentDir)) {
            rmdir($nonExistentDir);
        }
        $this->assertDirectoryDoesNotExist($nonExistentDir);

        $this->mockApplication([
            'id' => 'testapp',
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        ['host' => 'localhost'],
                    ],
                    'queues' => [
                        ['name' => 'test_queue'],
                    ],
                    'consumers' => [
                        [
                            'name' => 'test_consumer',
                            'callbacks' => [
                                'test_queue' => 'TestCallback',
                            ],
                            'systemd' => [
                                'memory_limit' => 0,
                                'workers' => 1,
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        $helper = new CreateUnitHelper([
            'units_dir' => $nonExistentDir,
            'user' => 'testuser',
            'group' => 'testgroup',
            'work_dir' => '/path/to/work',
        ]);
        $helper->init();

        // 验证目录已创建
        $this->assertDirectoryExists($nonExistentDir);
    }

    /**
     * 测试 bash 脚本内容
     */
    public function testBashScriptContent()
    {
        $this->mockApplication([
            'id' => 'testapp',
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        ['host' => 'localhost'],
                    ],
                    'queues' => [
                        ['name' => 'test_queue'],
                    ],
                    'consumers' => [
                        [
                            'name' => 'test_consumer',
                            'callbacks' => [
                                'test_queue' => 'TestCallback',
                            ],
                            'systemd' => [
                                'memory_limit' => 0,
                                'workers' => 1,
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        $helper = new CreateUnitHelper([
            'units_dir' => $this->testDir,
            'user' => 'testuser',
            'group' => 'testgroup',
            'work_dir' => '/path/to/work',
        ]);
        $helper->init();
        $helper->create();

        $bashFile = $this->testDir . '/exec.sh';
        $this->assertFileExists($bashFile);

        $content = file_get_contents($bashFile);
        $this->assertStringContainsString('MASK=consumer*.service', $content);
        $this->assertStringContainsString('systemctl', $content);
        $this->assertStringContainsString('copy', $content);
        $this->assertStringContainsString('start', $content);
        $this->assertStringContainsString('restart', $content);
        $this->assertStringContainsString('status', $content);
        $this->assertStringContainsString('delete', $content);
    }

    /**
     * 测试服务文件模板替换
     */
    public function testServiceFileTemplateReplacement()
    {
        $this->mockApplication([
            'id' => 'testapp',
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        ['host' => 'localhost'],
                    ],
                    'queues' => [
                        ['name' => 'test_queue'],
                    ],
                    'consumers' => [
                        [
                            'name' => 'my_consumer',
                            'callbacks' => [
                                'test_queue' => 'TestCallback',
                            ],
                            'systemd' => [
                                'memory_limit' => 64,
                                'workers' => 1,
                            ],
                        ],
                    ],
                ],
            ],
        ]);

        $helper = new CreateUnitHelper([
            'units_dir' => $this->testDir,
            'user' => 'appuser',
            'group' => 'appgroup',
            'work_dir' => '/var/www/app',
        ]);
        $helper->init();
        $helper->create();

        $serviceFile = $this->testDir . '/consumer_my_consumer_1.service';
        $content = file_get_contents($serviceFile);

        // 验证所有占位符都被正确替换
        $this->assertStringContainsString('Description=Consumer my_consumer', $content);
        $this->assertStringContainsString('WorkingDirectory=/var/www/app', $content);
        $this->assertStringContainsString('User=appuser', $content);
        $this->assertStringContainsString('Group=appgroup', $content);
        $this->assertStringContainsString('ExecStart=php /var/www/app/yii rabbitmq/consume my_consumer -l 64', $content);
        $this->assertStringContainsString('ExecReload=php /var/www/app/yii rabbitmq/restart-consume my_consumer -l 64', $content);
        $this->assertStringContainsString('Restart=always', $content);
    }

    /**
     * 测试没有消费者时不生成文件
     */
    public function testNoConsumers()
    {
        $this->mockApplication([
            'id' => 'testapp',
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        ['host' => 'localhost'],
                    ],
                    'consumers' => [],
                ],
            ],
        ]);

        $helper = new CreateUnitHelper([
            'units_dir' => $this->testDir,
            'user' => 'testuser',
            'group' => 'testgroup',
            'work_dir' => '/path/to/work',
        ]);
        $helper->init();
        $helper->create();

        // 验证没有生成服务文件
        $files = glob($this->testDir . '/consumer_*.service');
        $this->assertEmpty($files, 'No service files should be created when there are no consumers');

        // 当没有消费者时，$result 为空，不会生成 bash 脚本
        $bashFile = $this->testDir . '/exec.sh';
        $this->assertFileDoesNotExist($bashFile, 'Bash script should not be created when there are no consumers');
    }
}

