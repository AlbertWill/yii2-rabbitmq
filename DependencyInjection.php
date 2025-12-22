<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq;

use mikemadisonweb\rabbitmq\components\{
    AbstractConnectionFactory, Consumer, ConsumerInterface, Logger, Producer, Routing
};
use mikemadisonweb\rabbitmq\controllers\RabbitMQController;
use mikemadisonweb\rabbitmq\exceptions\InvalidConfigException;
use PhpAmqpLib\Connection\AbstractConnection;
use yii\base\Application;
use yii\base\BootstrapInterface;
use yii\di\NotInstantiableException;

class DependencyInjection implements BootstrapInterface
{
    /**
     * @var $logger Logger
     */
    private $logger;
    protected $isLoaded = false;

    /**
     * Configuration auto-loading
     * @param Application $app
     * @throws InvalidConfigException
     */
    public function bootstrap($app)
    {
        $config = $app->rabbitmq->getConfig();
        $this->registerLogger($config);
        $this->registerConnections($config);
        $this->registerRoutines($config);
        $this->registerProducers($config);
        $this->registerConsumers($config);
        $this->addControllers($app);
    }

    /**
     * Register logger service
     * @param $config
     */
    private function registerLogger($config)
    {
        \Yii::$container->setSingleton(Configuration::LOGGER_SERVICE_NAME, ['class' => Logger::class, 'options' => $config->logger]);
    }

    /**
     * Register connections in service container
     * @param Configuration $config
     */
    protected function registerConnections(Configuration $config)
    {
        foreach ($config->connections as $options) {
            $serviceAlias = sprintf(Configuration::CONNECTION_SERVICE_NAME, $options['name']);
            \Yii::$container->setSingleton($serviceAlias, function () use ($options) {
                $factory = new AbstractConnectionFactory($options['type'], $options);
                $connection = $factory->createConnection($options['name']);
                return $connection;
            });
        }
    }

    /**
     * Register routing in service container
     * @param Configuration $config
     */
    protected function registerRouting(Configuration $config)
    {
        \Yii::$container->setSingleton(Configuration::ROUTING_SERVICE_NAME, function ($container, $params) use ($config) {
            $routing = new Routing($params['conn']);
            \Yii::$container->invoke([$routing, 'setQueues'], [$config->queues]);
            \Yii::$container->invoke([$routing, 'setExchanges'], [$config->exchanges]);
            \Yii::$container->invoke([$routing, 'setBindings'], [$config->bindings]);

            return $routing;
        });
    }

    /**
     * 注册Routing容器
     * @param Configuration $config
     */
    protected function registerRoutines(Configuration $config)
    {
        foreach ($config->connections as $options) {
            $serviceAlias = sprintf(Configuration::ROUTING_SERVICE_NAME, $options['name']);
            \Yii::$container->setSingleton($serviceAlias, function ($container, $params) use ($config) {
                $routing = new Routing($params['conn']);

                //根据连接名称获取路由相关配置
                list($queues, $exchanges, $bindings) = $this->getRoutingConfigByConnName($params['conn']->name, $config);

                \Yii::$container->invoke([$routing, 'setQueues'], [$queues]);
                \Yii::$container->invoke([$routing, 'setExchanges'], [$exchanges]);
                \Yii::$container->invoke([$routing, 'setBindings'], [$bindings]);

                return $routing;
            });
        }
    }

    /**
     * Register producers in service container
     * @param Configuration $config
     */
    protected function registerProducers(Configuration $config)
    {
        $autoDeclare = $config->auto_declare;
        foreach ($config->producers as $options) {
            $serviceAlias = sprintf(Configuration::PRODUCER_SERVICE_NAME, $options['name']);
            \Yii::$container->setSingleton($serviceAlias, function () use ($options, $autoDeclare) {
                /**
                 * @var $connection AbstractConnection
                 */
                $connection = \Yii::$container->get(sprintf(Configuration::CONNECTION_SERVICE_NAME, $options['connection']));
                /**
                 * @var $routing Routing
                 */
                $routing = \Yii::$container->get(sprintf(Configuration::ROUTING_SERVICE_NAME, $options['connection']), ['conn' => $connection]);
                /**
                 * @var $logger Logger
                 */
                $logger = \Yii::$container->get(Configuration::LOGGER_SERVICE_NAME);
                $producer = new Producer($connection, $routing, $logger, $autoDeclare);
                \Yii::$container->invoke([$producer, 'setName'], [$options['name']]);
                \Yii::$container->invoke([$producer, 'setContentType'], [$options['content_type']]);
                \Yii::$container->invoke([$producer, 'setDeliveryMode'], [$options['delivery_mode']]);
                \Yii::$container->invoke([$producer, 'setSafe'], [$options['safe']]);
                \Yii::$container->invoke([$producer, 'setMaxReconnectAttempts'], [$options['max_reconnect_attempts']]);
                \Yii::$container->invoke([$producer, 'setReconnectDelay'], [$options['reconnect_delay']]);
                \Yii::$container->invoke([$producer, 'setSerializer'], [$options['serializer']]);

                return $producer;
            });
        }
    }

    /**
     * Register consumers(one instance per one or multiple queues) in service container
     * @param Configuration $config
     */
    protected function registerConsumers(Configuration $config)
    {
        $autoDeclare = $config->auto_declare;
        foreach ($config->consumers as $options) {
            $serviceAlias = sprintf(Configuration::CONSUMER_SERVICE_NAME, $options['name']);
            \Yii::$container->setSingleton($serviceAlias, function () use ($options, $autoDeclare) {
                /**
                 * @var $connection AbstractConnection
                 */
                $connection = \Yii::$container->get(sprintf(Configuration::CONNECTION_SERVICE_NAME, $options['connection']));
                /**
                 * @var $routing Routing
                 */
                $routing = \Yii::$container->get(sprintf(Configuration::ROUTING_SERVICE_NAME, $options['connection']), ['conn' => $connection]);
                /**
                 * @var $logger Logger
                 */
                $logger = \Yii::$container->get(Configuration::LOGGER_SERVICE_NAME);
                $consumer = new Consumer($connection, $routing, $logger, $autoDeclare);
                $queues = [];
                foreach ($options['callbacks'] as $queueName => $callback) {
                    $callbackClass = $this->getCallbackClass($callback);
                    $queues[$queueName] = [$callbackClass, 'execute'];
                }
                \Yii::$container->invoke([$consumer, 'setName'], [$options['name']]);
                \Yii::$container->invoke([$consumer, 'setQueues'], [$queues]);
                \Yii::$container->invoke([$consumer, 'setQos'], [$options['qos']]);
                \Yii::$container->invoke([$consumer, 'setIdleTimeout'], [$options['idle_timeout']]);
                \Yii::$container->invoke([$consumer, 'setIdleTimeoutExitCode'], [$options['idle_timeout_exit_code']]);
                \Yii::$container->invoke([$consumer, 'setProceedOnException'], [$options['proceed_on_exception']]);
                \Yii::$container->invoke([$consumer, 'setMaxReconnectAttempts'], [$options['max_reconnect_attempts']]);
                \Yii::$container->invoke([$consumer, 'setReconnectDelay'], [$options['reconnect_delay']]);
                \Yii::$container->invoke([$consumer, 'setDeserializer'], [$options['deserializer']]);

                return $consumer;
            });
        }
    }

    /**
     * Callback can be passed as class name or alias in service container
     * @param string $callbackName
     * @return ConsumerInterface
     * @throws InvalidConfigException
     */
    private function getCallbackClass(string $callbackName) : ConsumerInterface
    {
        if (!class_exists($callbackName)) {
            $callbackClass = \Yii::$container->get($callbackName);
        } else {
            $callbackClass = new $callbackName();
        }
        if (!($callbackClass instanceof ConsumerInterface)) {
            throw new InvalidConfigException("{$callbackName} should implement ConsumerInterface.");
        }

        return $callbackClass;
    }

    /**
     * Auto-configure console controller classes
     * @param Application $app
     */
    private function addControllers(Application $app)
    {
	    if($app instanceof \yii\console\Application) {
		    $app->controllerMap[Configuration::EXTENSION_CONTROLLER_ALIAS] = RabbitMQController::class;
	    }
    }

    /**
     * 根据连接名称获取路由相关配置
     * @param string $conn_name
     * @param Configuration $config
     * @return array[]
     */
    protected function getRoutingConfigByConnName(string $conn_name, Configuration $config){
        /**
         * 1、查找当前连接对应的队列名称
         */
        $queue_name_arr = [];
        $connection_config_arr = array_merge($config->producers, $config->consumers);
        foreach ($connection_config_arr as $cc_item){
            if($cc_item['connection'] == $conn_name && !in_array($cc_item['name'], $queue_name_arr)){
                $queue_name_arr[] = $cc_item['name'];
            }
        }

        /**
         * 2、过滤queues
         */
        $queues = [];
        foreach ($config->queues as $k=>$queue){
            if(in_array($queue['name'], $queue_name_arr)){
                $queues[] = $queue;
            }
        }

        /**
         * 3、过滤bindings
         */
        $bindings = [];
        $exchange_name_arr = [];//当前链接包含的交换机名称
        foreach ($config->bindings as $k=>$binding){
            if(in_array($binding['queue'], $queue_name_arr)){
                $bindings[] = $binding;
                if(!in_array($binding['exchange'], $exchange_name_arr)){
                    $exchange_name_arr[] = $binding['exchange'];
                }
            }
        }

        /**
         * 4、过滤exchanges
         */
        $exchanges = [];
        foreach ($config->exchanges as $k=>$exchange){
            if(in_array($exchange['name'], $exchange_name_arr)){
                $exchanges[] = $exchange;
            }
        }

        return [$queues, $exchanges, $bindings];
    }

    /**
     * 重新创建指定连接的实例（用于重连）
     * @param string $connectionName
     * @return object|string
     * @throws InvalidConfigException
     * @throws NotInstantiableException
     * @throws \yii\base\InvalidConfigException
     */
    public static function renewConnection(string $connectionName)
    {
        $app = \Yii::$app;
        $config = $app->rabbitmq->getConfig();
        $connections = $config->connections;

        $options = null;
        foreach ($connections as $connConfig) {
            if ($connConfig['name'] === $connectionName) {
                $options = $connConfig;
                break;
            }
        }

        if (empty($options)) {
            throw new InvalidConfigException("无法找到 RabbitMQ 连接配置: {$connectionName}");
        }

        $serviceAlias = sprintf(Configuration::CONNECTION_SERVICE_NAME, $connectionName);

        // 清除容器中的旧实例
        \Yii::$container->clear($serviceAlias);

        // 重新注册连接
        \Yii::$container->setSingleton($serviceAlias, function () use ($options) {
            $factory = new AbstractConnectionFactory($options['type'], $options);
            return $factory->createConnection($options['name']);
        });

        return \Yii::$container->get($serviceAlias);
    }

}
