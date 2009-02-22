#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <time.h>
#include <Zend/zend.h>
#include <php.h>
#include <php_ini.h>
#include "ext/standard/info.h"
#include "fastdht_client.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <fdht_client.h>
#include <fdht_func.h>
#include <logger.h>

/*
		ZEND_FE(fastdht_delete, NULL)
*/

// Every user visible function must have an entry in fastdht_client_functions[].
	function_entry fastdht_client_functions[] = {
		ZEND_FE(fastdht_set, NULL)
		ZEND_FE(fastdht_get, NULL)
		ZEND_FE(fastdht_inc, NULL)
		ZEND_FE(fastdht_batch_set, NULL)
		{NULL, NULL, NULL}  /* Must be the last line */
	};

zend_module_entry fastdht_client_module_entry = {
	STANDARD_MODULE_HEADER,
	"fastdht_client",
	fastdht_client_functions,
	PHP_MINIT(fastdht_client),
	PHP_MSHUTDOWN(fastdht_client),
	NULL,//PHP_RINIT(fastdht_client),
	NULL,//PHP_RSHUTDOWN(fastdht_client),
	PHP_MINFO(fastdht_client),
	"$Revision: 0.1 $", 
	STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_FASTDHT_CLIENT
	ZEND_GET_MODULE(fastdht_client)
#endif

#define FASTDHT_FILL_KEY(key_info, szNamespace, szObjectId, szKey) \
	if (key_info.namespace_len > FDHT_MAX_NAMESPACE_LEN) \
	{ \
		key_info.namespace_len = FDHT_MAX_NAMESPACE_LEN; \
	} \
	if (key_info.obj_id_len > FDHT_MAX_OBJECT_ID_LEN) \
	{ \
		key_info.obj_id_len = FDHT_MAX_OBJECT_ID_LEN; \
	} \
	if (key_info.key_len > FDHT_MAX_SUB_KEY_LEN) \
	{ \
		key_info.key_len = FDHT_MAX_SUB_KEY_LEN; \
	} \
 \
	memcpy(key_info.szNameSpace, szNamespace, key_info.namespace_len); \
	memcpy(key_info.szObjectId, szObjectId, key_info.obj_id_len); \
	memcpy(key_info.szKey, szKey, key_info.key_len); \


#define FASTDHT_FILL_OBJECT(obj_info, szNamespace, szObjectId) \
	if (obj_info.namespace_len > FDHT_MAX_NAMESPACE_LEN) \
	{ \
		obj_info.namespace_len = FDHT_MAX_NAMESPACE_LEN; \
	} \
	if (obj_info.obj_id_len > FDHT_MAX_OBJECT_ID_LEN) \
	{ \
		obj_info.obj_id_len = FDHT_MAX_OBJECT_ID_LEN; \
	} \
 \
	memcpy(obj_info.szNameSpace, szNamespace, obj_info.namespace_len); \
	memcpy(obj_info.szObjectId, szObjectId, obj_info.obj_id_len); \

/*
int fastdht_set(string namespace, string object_id, string key, 
		string value [, int expires])
return 0 for success, != 0 for error
*/
ZEND_FUNCTION(fastdht_set)
{
	int argc;
	char *szNamespace;
	char *szObjectId;
	char *szKey;
	char *szValue;
	int value_len;
	long expires;
	FDHTKeyInfo key_info;

	argc = ZEND_NUM_ARGS();
	if (argc != 4 && argc != 5)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_set parameters count: %d != 4 or 5", 
			__LINE__, argc);
		RETURN_LONG(EINVAL);
	}

	expires = FDHT_EXPIRES_NEVER;
	if (zend_parse_parameters(argc TSRMLS_CC, "ssss|l", &szNamespace, 
		&key_info.namespace_len, &szObjectId, &key_info.obj_id_len, 
		&szKey, &key_info.key_len, &szValue, &value_len, &expires)
		 == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_set parameter parse error!", __LINE__);
		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_KEY(key_info, szNamespace, szObjectId, szKey)

	/*
	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), szKey=%s(%d), "
		"szValue=%s(%d), expires=%ld", 
		szNamespace, key_info.namespace_len, 
		szObjectId, key_info.obj_id_len, 
		szKey, key_info.key_len,
		szValue, value_len, expires);
	*/

	RETURN_LONG(fdht_set(&key_info, expires, szValue, value_len));
}

/*
int fastdht_batch_set(string namespace, string object_id, array key_value_hash, 
		[, int expires])
return 0 for success, != 0 for error
*/
ZEND_FUNCTION(fastdht_batch_set)
{
	int argc;
	char *szNamespace;
	char *szObjectId;
	zval *key_values;
	HashTable *key_value_hash;
	zval **data;
	zval ***ppp;
	int key_count;
	char *szKey;
	long index;
	long expires;
	int result;
	FDHTObjectInfo obj_info;
	FDHTKeyValuePair key_list[FDHT_MAX_KEY_COUNT_PER_REQ];
	zval zvalues[FDHT_MAX_KEY_COUNT_PER_REQ];
	zval *pValue;
	zval *pValueEnd;
	FDHTKeyValuePair *pKeyValuePair;
	FDHTKeyValuePair *pKeyValueEnd;
	HashPosition pointer;

	argc = ZEND_NUM_ARGS();
	if (argc != 3 && argc != 4)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_set parameters count: %d != 3 or 4", \
			__LINE__, argc);
		RETURN_LONG(EINVAL);
	}

	expires = FDHT_EXPIRES_NEVER;
	if (zend_parse_parameters(argc TSRMLS_CC, "ssa|l", &szNamespace, 
		&obj_info.namespace_len, &szObjectId, &obj_info.obj_id_len, 
		&key_values, &expires) == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_batch_set parameter parse error!", __LINE__);
		RETURN_LONG(EINVAL);
	}

	key_value_hash = Z_ARRVAL_P(key_values);
	key_count = zend_hash_num_elements(key_value_hash);
	if (key_count <= 0 || key_count > FDHT_MAX_KEY_COUNT_PER_REQ)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_batch_set, invalid key_count: %d!", \
			__LINE__, key_count);
		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_OBJECT(obj_info, szNamespace, szObjectId)

	/*
	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), "
		"expires=%ld", szNamespace, obj_info.namespace_len, 
		szObjectId, obj_info.obj_id_len, expires);
	*/

	memset(zvalues, 0, sizeof(zvalues));
	memset(key_list, 0, sizeof(key_list));
	pValue = zvalues;
	pKeyValuePair = key_list;
	ppp = &data;
	for (zend_hash_internal_pointer_reset_ex(key_value_hash, &pointer);
	     zend_hash_get_current_data_ex(key_value_hash, (void **)ppp,
		&pointer) == SUCCESS; zend_hash_move_forward_ex(
		key_value_hash, &pointer))
	{
		if (zend_hash_get_current_key_ex(key_value_hash, &szKey, \
			&(pKeyValuePair->key_len), &index, 0, &pointer) \
			!= HASH_KEY_IS_STRING)
		{
			logError("file: "__FILE__", line: %d, " \
				"fastdht_batch_set, invalid array element, " \
				"index=%d!", __LINE__, index);
			RETURN_LONG(EINVAL);
		}

		if (pKeyValuePair->key_len > FDHT_MAX_SUB_KEY_LEN)
		{
			pKeyValuePair->key_len = FDHT_MAX_SUB_KEY_LEN;
		}
		memcpy(pKeyValuePair->szKey, szKey, pKeyValuePair->key_len);

		if (Z_TYPE_PP(data) == IS_STRING)
		{
			pKeyValuePair->pValue = Z_STRVAL_PP(data);
			pKeyValuePair->value_len = Z_STRLEN_PP(data);
		}
		else
		{
			*pValue = **data;
			zval_copy_ctor(pValue);
			convert_to_string(pValue);
			pKeyValuePair->pValue = Z_STRVAL(*pValue);
			pKeyValuePair->value_len = Z_STRLEN(*pValue);

			pValue++;
		}

		/*
		logInfo("key=%s(%d), value=%s(%d)", \
			pKeyValuePair->szKey, pKeyValuePair->key_len, \
			pKeyValuePair->pValue, pKeyValuePair->value_len);
		*/

		pKeyValuePair++;
	}
	pValueEnd = pValue;

	result = fdht_batch_set(&obj_info, key_list, key_count, expires);
	for (pValue=zvalues; pValue<pValueEnd; pValue++)
	{
		zval_dtor(pValue);
	}

	if (result != 0)
	{
		RETURN_LONG(result);
	}

	array_init(return_value);

	pKeyValueEnd = key_list + key_count;
	for (pKeyValuePair=key_list; pKeyValuePair<pKeyValueEnd; pKeyValuePair++)
	{
		add_assoc_long(return_value, pKeyValuePair->szKey, \
				pKeyValuePair->status);
	}
}

/*
string/int fastdht_get(string namespace, string object_id, string key
		[, int expires])
return string value for success, int value (errno) for error
*/
ZEND_FUNCTION(fastdht_get)
{
	int argc;
	char *szNamespace;
	char *szObjectId;
	char *szKey;
	char *pValue;
	int value_len;
	long expires;
	int result;
	FDHTKeyInfo key_info;
	
	argc = ZEND_NUM_ARGS();
	if (argc != 3 && argc != 4)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_get parameters: %d != 3 or 4", 
			__LINE__, argc);

		RETURN_LONG(EINVAL);
	}

	expires = FDHT_EXPIRES_NONE;
	if (zend_parse_parameters(argc TSRMLS_CC, "sss|l", &szNamespace, 
		&key_info.namespace_len, &szObjectId, &key_info.obj_id_len, 
		&szKey, &key_info.key_len, &expires)
		 == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_get parameter parse error!", __LINE__);

		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_KEY(key_info, szNamespace, szObjectId, szKey)

	/*
	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), szKey=%s(%d), expires=%ld", 
		szNamespace, key_info.namespace_len, 
		szObjectId, key_info.obj_id_len, 
		szKey, key_info.key_len, expires);
	*/

	pValue = NULL;
	value_len = 0;
	if ((result=fdht_get_ex1(&g_group_array, g_keep_alive, &key_info, \
			expires, &pValue, &value_len, _emalloc)) != 0)
	{
		RETURN_LONG(result);
	}

	RETURN_STRING(pValue, 0);
}

/*
string/int fastdht_inc(string namespace, string object_id, string key, 
		int increment [, int expires])
return string value for success, int value (errno) for error
*/
ZEND_FUNCTION(fastdht_inc)
{
	int argc;
	char *szNamespace;
	char *szObjectId;
	char *szKey;
	char szValue[32];
	int value_len;
	long increment;
	long expires;
	int result;
	FDHTKeyInfo key_info;
	
	argc = ZEND_NUM_ARGS();
	if (argc != 4 && argc != 5)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_inc parameters: %d != 4 or 5", 
			__LINE__, argc);

		RETURN_LONG(EINVAL);
	}

	expires = FDHT_EXPIRES_NEVER;
	if (zend_parse_parameters(argc TSRMLS_CC, "sssl|l", &szNamespace, 
		&key_info.namespace_len, &szObjectId, &key_info.obj_id_len, 
		&szKey, &key_info.key_len, &increment, &expires)
		 == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_inc parameter parse error!", __LINE__);

		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_KEY(key_info, szNamespace, szObjectId, szKey)

	/*
	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), szKey=%s(%d), "
		"increment=%ld, expires=%ld", 
		szNamespace, key_info.namespace_len, 
		szObjectId, key_info.obj_id_len, 
		szKey, key_info.key_len, increment, expires);
	*/

	value_len = sizeof(szValue);
	if ((result=fdht_inc(&key_info, expires, increment, 
			szValue, &value_len)) != 0)
	{
		RETURN_LONG(result);
	}

	RETURN_STRING(szValue, 1);
}

/*
int fastdht_delete(string namespace, string object_id, string key)
return 0 for success, != 0 for error
*/
ZEND_FUNCTION(fastdht_delete)
{
	int argc;
	char *szNamespace;
	char *szObjectId;
	char *szKey;
	FDHTKeyInfo key_info;
	
	argc = ZEND_NUM_ARGS();
	if (argc != 3)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_delete parameters: %d != 3", 
			__LINE__, argc);

		RETURN_LONG(EINVAL);
	}

	if (zend_parse_parameters(argc TSRMLS_CC, "sss", &szNamespace, 
		&key_info.namespace_len, &szObjectId, &key_info.obj_id_len, 
		&szKey, &key_info.key_len) == FAILURE)
	{
		logError("file: "__FILE__", line: %d, " \
			"fastdht_delete parameter parse error!", __LINE__);

		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_KEY(key_info, szNamespace, szObjectId, szKey)

	/*
	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), szKey=%s(%d)", 
		szNamespace, key_info.namespace_len, 
		szObjectId, key_info.obj_id_len, 
		szKey, key_info.key_len);
	*/

	RETURN_LONG(fdht_delete(&key_info));
}

PHP_MINIT_FUNCTION(fastdht_client)
{
	#define ITEM_NAME_CONF_FILE "fastdht_client.config_file"
	zval conf_filename;

	if (zend_get_configuration_directive(ITEM_NAME_CONF_FILE, 
		sizeof(ITEM_NAME_CONF_FILE), &conf_filename) != SUCCESS)
	{
		fprintf(stderr, "file: "__FILE__", line: %d, " \
			"get param: %s from fastdht_client.ini fail!\n", 
			__LINE__, ITEM_NAME_CONF_FILE);

		return FAILURE;
	}

	if (fdht_client_init(conf_filename.value.str.val) != 0)
	{
		return FAILURE;
	}

	REGISTER_LONG_CONSTANT("FDHT_EXPIRES_NEVER", 0, CONST_CS|CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("FDHT_EXPIRES_NONE", -1, CONST_CS|CONST_PERSISTENT);

	return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(fastdht_client)
{
	if (g_keep_alive)
	{
		fdht_disconnect_all_servers(&g_group_array);
	}

	fdht_client_destroy();

	return SUCCESS;
}

PHP_RINIT_FUNCTION(fastdht_client)
{
	return SUCCESS;
}

PHP_RSHUTDOWN_FUNCTION(fastdht_client)
{
	fprintf(stderr, "request shut down. file: "__FILE__", line: %d\n", __LINE__);
	return SUCCESS;
}

PHP_MINFO_FUNCTION(fastdht_client)
{
	php_info_print_table_start();
	php_info_print_table_header(2, "fastdht_client support", "enabled");
	php_info_print_table_end();

}

