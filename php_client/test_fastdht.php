<?php

$namespace = '';
$object_id = '';
$key = 'key';
$value = 'this is a test';

if (($result=fastdht_set($namespace, $object_id, $key, $value)) != 0)
{
	error_log("fastdht_set fail, errno: $result");
}

$value = fastdht_get($namespace, $object_id, $key);
if (!is_string($value))
{
	error_log("fastdht_get fail, errno: $value");
}
else
{
	echo "value: $value\n";
}
?>
