<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests;

use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\exceptions\InvalidConfigException;
use yii\base\UnknownPropertyException;

class ConfigurationTest extends TestCase
{
    protected $invalidConfig = [];

    public function testIsNotConfigured()
    {
        $this->expectException(UnknownPropertyException::class);
        $this->mockApplication();
        \Yii::$app->rabbitmq->getConfig();
    }

    public function testConfigType()
    {
        $this->mockApplication([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'host' => 'localhost',
                        ],
                    ],
                ],
            ],
        ]);
        $this->assertInstanceOf(Configuration::class, \Yii::$app->rabbitmq->getConfig());
    }

    /**
     * @dataProvider invalidConfig
     */
    public function testInvalidConfig($reason, $config, $exception)
    {
        $this->expectException($exception);
        $rabbitmq = array_merge(['class' => Configuration::class], $config);
        $components = [
            'components' => [
                'rabbitmq' => $rabbitmq,
            ],
        ];
        $this->mockApplication($components);
        \Yii::$app->rabbitmq->getConfig();
    }

    public function invalidConfig()
    {
        $required = ['connections' => [['host' => 'localhost']]];

        return [
            ['At least one connection required', ['connections' => []], InvalidConfigException::class],
            ['Unknown option', array_merge($required, ['unknown' => []]), UnknownPropertyException::class],
            ['Name should be specified on multiple connections', ['connections' => [['host' => 'rabbitmq'],['host' => 'rabbitmq']]], InvalidConfigException::class],
            ['Wrong auto-declare option', array_merge($required, ['auto_declare' => 43]), InvalidConfigException::class],
            ['Logger is not an array', array_merge($required, ['logger' => 43]), InvalidConfigException::class],
            ['Unknown option in logger section', array_merge($required, ['logger' => ['unknown' => 43]]), InvalidConfigException::class],
            ['Connections array should be multidimensional and numeric', ['connections' => ['some-key' => 'some-value']], InvalidConfigException::class],
            ['Bindings array should be multidimensional and numeric', ['bindings' => ['some-key' => 'some-value']], InvalidConfigException::class],
            ['Exchanges array should be multidimensional and numeric', ['exchanges' => ['some-key' => 'some-value']], InvalidConfigException::class],
            ['Queues array should be multidimensional and numeric', ['queues' => ['some-key' => 'some-value']], InvalidConfigException::class],
            ['Producers array should be multidimensional and numeric', ['producers' => ['some-key' => 'some-value']], InvalidConfigException::class],
            ['Consumers array should be multidimensional and numeric', ['consumers' => ['some-key' => 'some-value']], InvalidConfigException::class],
            ['Url or host is required', ['connections' => [[]]], InvalidConfigException::class],
            ['Not both url and host allowed', ['connections' => [['url' => 'some-value1', 'host' => 'some-value2']]], InvalidConfigException::class],
            ['No additional fields are allowed in connections', ['connections' => [['wrong' => 'wrong', 'host' => 'localhost']]], InvalidConfigException::class],
            ['Connection type should be of correct subclass', ['connections' => [['type' => 'SomeTypeUnknown', 'host' => 'localhost']]], InvalidConfigException::class],
            ['Exchange name is required', array_merge($required, ['exchanges' => [['type' => 'direct']]]), InvalidConfigException::class],
            ['Exchange type is required', array_merge($required, ['exchanges' => [['name' => 'direct']]]), InvalidConfigException::class],
            ['Exchange type should be one of allowed', array_merge($required, ['exchanges' => [['type' => 'wrong']]]), InvalidConfigException::class],
            ['Exchange wrong field', array_merge($required, ['exchanges' => [['type' => 'direct', 'name' => 'direct', 'wrong' => 'wrong']]]), InvalidConfigException::class],
            ['Queue wrong field', array_merge($required, ['queues' => [['wrong' => 'wrong']]]), InvalidConfigException::class],
            ['Exchange name is required for binding', array_merge($required, ['bindings' => [[]]]), InvalidConfigException::class],
            ['Routing key is required for binding', array_merge($required, ['bindings' => [['exchange' => 'smth']]]), InvalidConfigException::class],
            ['Either `queue` or `to_exchange` options should be specified to create binding', array_merge($required, ['bindings' => [['exchange' => 'smth', 'routing_keys' => ['smth'],]]]), InvalidConfigException::class],
            ['Exchanges and queues should be configured in corresponding sections', array_merge($required, ['bindings' => [['exchange' => 'smth', 'routing_keys' => ['smth'], 'queue' => 'smth',]]]), InvalidConfigException::class],
            ['Binding wrong field', array_merge($required, ['bindings' => [['wrong' => 'wrong']]]), InvalidConfigException::class],
            ['Producer wrong field', array_merge($required, ['producers' => [['wrong' => 'wrong']]]), InvalidConfigException::class],
            ['Producer name is required', array_merge($required, ['producers' => [[]]]), InvalidConfigException::class],
            ['Connection defined in producer should exist', array_merge($required, ['producers' => [['name' => 'smth', 'connection' => 'unknown']]]), InvalidConfigException::class],
            ['Safe option should be a boolean', array_merge($required, ['producers' => [['name' => 'smth', 'safe' => 'non_bool']]]), InvalidConfigException::class],
            ['Serializer should be callable', array_merge($required, ['producers' => [['name' => 'smth', 'serializer' => 'non_callable']]]), InvalidConfigException::class],
            ['Consumer wrong field', array_merge($required, ['consumers' => [['wrong' => 'wrong']]]), InvalidConfigException::class],
            ['Consumer name is required', array_merge($required, ['consumers' => [[]]]), InvalidConfigException::class],
            ['Connection defined in consumer should exist', array_merge($required, ['consumers' => [['name' => 'smth', 'connection' => 'unknown']]]), InvalidConfigException::class],
            ['No callbacks specified in consumer', array_merge($required, ['consumers' => [['name' => 'smth', 'callbacks' => []]]]), InvalidConfigException::class],
            ['Option qos is not an array', array_merge($required, ['consumers' => [['name' => 'smth', 'qos' => 'not_an_array']]]), InvalidConfigException::class],
            ['Option proceed_on_exception is not a boolean', array_merge($required, ['consumers' => [['name' => 'smth', 'proceed_on_exception' => 'not_a_boolean']]]), InvalidConfigException::class],
            ['Queue defined in consumer should exist', array_merge($required, ['consumers' => [['name' => 'smth', 'callbacks' => ['unknown' => 'some-callback']]]]), InvalidConfigException::class],
            ['Consumer callback parameter should be string', array_merge($required, ['queues' => [['name' => 'smth']], 'consumers' => [['name' => 'smth', 'callbacks' => ['smth' => 34]]]]), InvalidConfigException::class],
            ['Deserializer should be callable', array_merge($required, ['consumers' => [['name' => 'smth', 'deserializer' => 'non_callable']]]), InvalidConfigException::class],
            ['Connection in consumer not exist', ['connections' => [['host' => 'rabbitmq']], 'consumers' => [['name' => 'smth', 'connection' => 'default2']]], InvalidConfigException::class],
            ['Named connection not specified in producer', ['connections' => [['host' => 'rabbitmq', 'name' => 'default2']], 'producers' => [['name' => 'smth']]], InvalidConfigException::class],
            ['Duplicate names in producer', array_merge($required, ['producers' => [['name' => 'smth'], ['name' => 'smth']]]), InvalidConfigException::class],
            // Semaphore configuration tests
            ['Semaphore is not an array', array_merge($required, ['semaphore' => 'not_an_array']), InvalidConfigException::class],
            ['Semaphore type is not a string', array_merge($required, ['semaphore' => ['type' => 123]]), InvalidConfigException::class],
            ['Semaphore class does not exist', array_merge($required, ['semaphore' => ['type' => 'NonExistentClass']]), InvalidConfigException::class],
            ['Semaphore class must extend Semaphore', array_merge($required, ['semaphore' => ['type' => 'stdClass']]), InvalidConfigException::class],
            ['Semaphore redis_component_name is not a string', array_merge($required, ['semaphore' => ['redis_component_name' => 123]]), InvalidConfigException::class],
            ['Semaphore limit is not an integer', array_merge($required, ['semaphore' => ['limit' => 'not_int']]), InvalidConfigException::class],
            ['Semaphore ttl is not a positive integer', array_merge($required, ['semaphore' => ['ttl' => 0]]), InvalidConfigException::class],
            ['Semaphore ttl is not a positive integer (negative)', array_merge($required, ['semaphore' => ['ttl' => -1]]), InvalidConfigException::class],
            ['Semaphore ttl is not an integer', array_merge($required, ['semaphore' => ['ttl' => 'not_int']]), InvalidConfigException::class],
            ['Semaphore acquire_sleep is not a positive integer', array_merge($required, ['semaphore' => ['acquire_sleep' => -1]]), InvalidConfigException::class],
            ['Semaphore acquire_sleep is not a positive integer (zero)', array_merge($required, ['semaphore' => ['acquire_sleep' => 0]]), InvalidConfigException::class],
            ['Semaphore acquire_sleep is not an integer', array_merge($required, ['semaphore' => ['acquire_sleep' => 'not_int']]), InvalidConfigException::class],
            // Consumer semaphore configuration tests
            ['Consumer semaphore is not an array', array_merge($required, ['consumers' => [['name' => 'smth', 'callbacks' => ['queue' => 'callback'], 'semaphore' => 'not_an_array']]]), InvalidConfigException::class],
            ['Consumer semaphore contains unknown option', array_merge($required, ['queues' => [['name' => 'queue']], 'consumers' => [['name' => 'smth', 'callbacks' => ['queue' => 'callback'], 'semaphore' => ['unknown' => 'value']]]]), InvalidConfigException::class],
            ['Consumer semaphore type is not a string', array_merge($required, ['queues' => [['name' => 'queue']], 'consumers' => [['name' => 'smth', 'callbacks' => ['queue' => 'callback'], 'semaphore' => ['type' => 123]]]]), InvalidConfigException::class],
            ['Consumer semaphore class does not exist', array_merge($required, ['queues' => [['name' => 'queue']], 'consumers' => [['name' => 'smth', 'callbacks' => ['queue' => 'callback'], 'semaphore' => ['type' => 'NonExistentClass']]]]), InvalidConfigException::class],
            ['Consumer semaphore class must extend Semaphore', array_merge($required, ['queues' => [['name' => 'queue']], 'consumers' => [['name' => 'smth', 'callbacks' => ['queue' => 'callback'], 'semaphore' => ['type' => 'stdClass']]]]), InvalidConfigException::class],
            ['Consumer semaphore limit is not an integer', array_merge($required, ['queues' => [['name' => 'queue']], 'consumers' => [['name' => 'smth', 'callbacks' => ['queue' => 'callback'], 'semaphore' => ['limit' => 'not_int']]]]), InvalidConfigException::class],
            ['Consumer semaphore ttl is not a positive integer', array_merge($required, ['queues' => [['name' => 'queue']], 'consumers' => [['name' => 'smth', 'callbacks' => ['queue' => 'callback'], 'semaphore' => ['ttl' => 0]]]]), InvalidConfigException::class],
            ['Consumer semaphore ttl is not an integer', array_merge($required, ['queues' => [['name' => 'queue']], 'consumers' => [['name' => 'smth', 'callbacks' => ['queue' => 'callback'], 'semaphore' => ['ttl' => 'not_int']]]]), InvalidConfigException::class],
            ['Consumer semaphore acquire_sleep is not a positive integer', array_merge($required, ['queues' => [['name' => 'queue']], 'consumers' => [['name' => 'smth', 'callbacks' => ['queue' => 'callback'], 'semaphore' => ['acquire_sleep' => -1]]]]), InvalidConfigException::class],
            ['Consumer semaphore acquire_sleep is not a positive integer (zero)', array_merge($required, ['queues' => [['name' => 'queue']], 'consumers' => [['name' => 'smth', 'callbacks' => ['queue' => 'callback'], 'semaphore' => ['acquire_sleep' => 0]]]]), InvalidConfigException::class],
            ['Consumer semaphore acquire_sleep is not an integer', array_merge($required, ['queues' => [['name' => 'queue']], 'consumers' => [['name' => 'smth', 'callbacks' => ['queue' => 'callback'], 'semaphore' => ['acquire_sleep' => 'not_int']]]]), InvalidConfigException::class],
        ];
    }

    public function testValidSemaphoreConfig()
    {
        $this->mockApplication([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'host' => 'localhost',
                        ],
                    ],
                    'semaphore' => [
                        'type' => \mikemadisonweb\rabbitmq\components\semaphore\HashSemaphore::class,
                        'redis_component_name' => 'redis',
                        'limit' => 10,
                        'ttl' => 300,
                        'acquire_sleep' => 60,
                    ],
                ],
            ],
        ]);
        $config = \Yii::$app->rabbitmq->getConfig();
        $this->assertInstanceOf(Configuration::class, $config);
        $this->assertEquals(\mikemadisonweb\rabbitmq\components\semaphore\HashSemaphore::class, $config->semaphore['type']);
        $this->assertEquals('redis', $config->semaphore['redis_component_name']);
        $this->assertEquals(10, $config->semaphore['limit']);
        $this->assertEquals(300, $config->semaphore['ttl']);
        $this->assertEquals(60, $config->semaphore['acquire_sleep']);
    }

    public function testValidConsumerSemaphoreConfig()
    {
        $this->mockApplication([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'host' => 'localhost',
                        ],
                    ],
                    'queues' => [
                        [
                            'name' => 'test-queue',
                        ],
                    ],
                    'consumers' => [
                        [
                            'name' => 'test-consumer',
                            'callbacks' => [
                                'test-queue' => 'callback',
                            ],
                            'semaphore' => [
                                'type' => \mikemadisonweb\rabbitmq\components\semaphore\IncrSemaphore::class,
                                'limit' => 5,
                                'ttl' => 600,
                                'acquire_sleep' => 30,
                            ],
                        ],
                    ],
                ],
            ],
        ]);
        $config = \Yii::$app->rabbitmq->getConfig();
        $this->assertInstanceOf(Configuration::class, $config);
        $consumer = $config->consumers[0];
        $this->assertEquals(\mikemadisonweb\rabbitmq\components\semaphore\IncrSemaphore::class, $consumer['semaphore']['type']);
        $this->assertEquals(5, $consumer['semaphore']['limit']);
        $this->assertEquals(600, $consumer['semaphore']['ttl']);
        $this->assertEquals(30, $consumer['semaphore']['acquire_sleep']);
    }
}
