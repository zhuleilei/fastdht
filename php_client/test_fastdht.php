<?php

$namespace = '';
$object_id = '';
$key = 'key';
$value = 1234;

$fdht = new FastDHT(true);
if (($result=$fdht->set($namespace, $object_id, $key, $value)) != 0)
{
	error_log("fastdht_set fail, errno: $result");
}
var_dump($fdht->get($namespace, $object_id, $key));
var_dump($fdht->inc($namespace, $object_id, $key, 10));

echo 'delete: ' . $fdht->delete($namespace, $object_id, $key) . "\n";

$fdht->close();

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
