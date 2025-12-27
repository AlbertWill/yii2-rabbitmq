<?php
// ensure we get report on all possible php errors
error_reporting(-1);
define('YII_ENABLE_ERROR_HANDLER', false);
// 在测试环境中禁用调试输出，避免日志干扰测试输出
// 如果需要调试，可以通过环境变量 PHPUNIT_YII_DEBUG=1 启用
define('YII_DEBUG', getenv('PHPUNIT_YII_DEBUG') === '1');
$_SERVER['SCRIPT_NAME']     = '/' . __DIR__;
$_SERVER['SCRIPT_FILENAME'] = __FILE__;

// 优先查找本地 vendor（不是符号链接），如果不存在则查找外层 vendor
$localVendor = __DIR__ . '/../vendor/autoload.php';
$parentVendor = dirname(__DIR__, 2) . '/vendor/autoload.php';

$composerAutoload = null;
if (is_file($localVendor) && !is_link(dirname($localVendor))) {
    // 使用本地 vendor（不是符号链接）
    $composerAutoload = $localVendor;
} elseif (is_file($parentVendor)) {
    // 使用外层 vendor
    $composerAutoload = $parentVendor;
}

if (!$composerAutoload || !is_file($composerAutoload)) {
    die("Composer autoloader not found! Please run 'composer install' in the extension directory or ensure parent project vendor is available.\n");
}

require_once($composerAutoload);

// 同样处理 Yii.php
$localYii = __DIR__ . '/../vendor/yiisoft/yii2/Yii.php';
$parentYii = dirname(__DIR__, 2) . '/vendor/yiisoft/yii2/Yii.php';

if (is_file($localYii) && !is_link(dirname($localYii))) {
    require_once($localYii);
} elseif (is_file($parentYii)) {
    require_once($parentYii);
} else {
    die("Yii.php not found! Please ensure Yii2 framework is installed.\n");
}

Yii::setAlias('@mikemadisonweb/rabbitmq/tests', __DIR__);
