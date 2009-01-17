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

#define FDHT_CLIENT_CONF_FILENAME  "/home/yuqing/fastdht/conf/fdht_client.conf"

// Every user visible function must have an entry in fastdht_client_functions[].
	function_entry fastdht_client_functions[] = {
		ZEND_FE(fastdht_set, NULL)
		ZEND_FE(fastdht_get, NULL)
		ZEND_FE(fastdht_inc, NULL)
		ZEND_FE(fastdht_delete, NULL)
		{NULL, NULL, NULL}  /* Must be the last line */
	};

// {{{ fastdht_client_module_entry 
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


/*
int fastdht_set(string namespace, string object_id, string key, 
		string value [, int expires])
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
		php_error(E_ERROR, "fastdht_set parameters count: %d != 4 or 5"
			, argc);
		RETURN_LONG(EINVAL);
	}

	memset(&key_info, 0, sizeof(FDHTKeyInfo));

	expires = FDHT_EXPIRES_NEVER;
	if (zend_parse_parameters(argc TSRMLS_CC, "ssss|l", &szNamespace, 
		&key_info.namespace_len, &szObjectId, &key_info.obj_id_len, 
		&szKey, &key_info.key_len, &szValue, &value_len, &expires)
		 == FAILURE)
	{
		php_error(E_ERROR, "fastdht_set parameter parse error!");
		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_KEY(key_info, szNamespace, szObjectId, szKey)

	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), szKey=%s(%d), "
		"szValue=%s(%d), expires=%ld", 
		key_info.szNameSpace, key_info.namespace_len, 
		key_info.szObjectId, key_info.obj_id_len, 
		key_info.szKey, key_info.key_len,
		szValue, value_len, expires);

	RETURN_LONG(fdht_set(&key_info, expires, szValue, value_len));
}

/*
string fastdht_get(string namespace, string object_id, string key
		[, int expires])
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
		php_error(E_ERROR, "fastdht_set parameters: %d != 3 or 4", argc);
		RETURN_LONG(EINVAL);
	}

	expires = FDHT_EXPIRES_NONE;
	memset(&key_info, 0, sizeof(FDHTKeyInfo));
	if (zend_parse_parameters(argc TSRMLS_CC, "sss|l", &szNamespace, 
		&key_info.namespace_len, &szObjectId, &key_info.obj_id_len, 
		&szKey, &key_info.key_len, &expires)
		 == FAILURE)
	{
		php_error(E_ERROR, "fastdht_set parameter parse error!");
		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_KEY(key_info, szNamespace, szObjectId, szKey)

	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), szKey=%s(%d), expires=%d", 
		key_info.szNameSpace, key_info.namespace_len, 
		key_info.szObjectId, key_info.obj_id_len, 
		key_info.szKey, key_info.key_len, expires);

	pValue = NULL;
	value_len = 0;
	if ((result=fdht_get_ex1(&key_info, expires, &pValue, &value_len, 
				_emalloc)) != 0)
	{
		RETURN_LONG(result);
	}

	RETURN_STRING(pValue, 0);
}

/*
string fastdht_inc(string namespace, string object_id, string key, 
		int increment [, int expires])
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
		php_error(E_ERROR, "fastdht_set parameters: %d != 4 or 5", argc);
		RETURN_LONG(EINVAL);
	}

	expires = FDHT_EXPIRES_NEVER;
	memset(&key_info, 0, sizeof(FDHTKeyInfo));
	if (zend_parse_parameters(argc TSRMLS_CC, "sssl|l", &szNamespace, 
		&key_info.namespace_len, &szObjectId, &key_info.obj_id_len, 
		&szKey, &key_info.key_len, &increment, &expires)
		 == FAILURE)
	{
		php_error(E_ERROR, "fastdht_set parameter parse error!");
		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_KEY(key_info, szNamespace, szObjectId, szKey)

	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), szKey=%s(%d), "
		"increment=%ld, expires=%ld", 
		key_info.szNameSpace, key_info.namespace_len, 
		key_info.szObjectId, key_info.obj_id_len, 
		key_info.szKey, key_info.key_len, increment, expires);

	value_len = sizeof(szValue);
	if ((result=fdht_inc(&key_info, expires, increment, 
			szValue, &value_len)) != 0)
	{
		RETURN_LONG(result);
	}

	RETURN_STRING(szValue, 1);
}

/*
string fastdht_delete(string namespace, string object_id, string key)
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
		php_error(E_ERROR, "fastdht_delete parameters: %d != 3", argc);
		RETURN_LONG(EINVAL);
	}

	memset(&key_info, 0, sizeof(FDHTKeyInfo));
	if (zend_parse_parameters(argc TSRMLS_CC, "sss", &szNamespace, 
		&key_info.namespace_len, &szObjectId, &key_info.obj_id_len, 
		&szKey, &key_info.key_len) == FAILURE)
	{
		php_error(E_ERROR, "fastdht_delete parameter parse error!");
		RETURN_LONG(EINVAL);
	}

	FASTDHT_FILL_KEY(key_info, szNamespace, szObjectId, szKey)

	logInfo("szNamespace=%s(%d), szObjectId=%s(%d), szKey=%s(%d)", 
		key_info.szNameSpace, key_info.namespace_len, 
		key_info.szObjectId, key_info.obj_id_len, 
		key_info.szKey, key_info.key_len);

	RETURN_LONG(fdht_delete(&key_info));
}

// {{{ PHP_MINIT_FUNCTION
PHP_MINIT_FUNCTION(fastdht_client)
{
	int result;

	if ((result=fdht_client_init(FDHT_CLIENT_CONF_FILENAME)) != 0)
	{
		return result;
	}

	REGISTER_LONG_CONSTANT("FDHT_EXPIRES_NEVER", 0, CONST_CS|CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("FDHT_EXPIRES_NONE", -1, CONST_CS|CONST_PERSISTENT);

	return SUCCESS;
}
// }}} 

// {{{ PHP_MSHUTDOWN_FUNCTION
PHP_MSHUTDOWN_FUNCTION(fastdht_client)
{
	if (g_keep_alive)
	{
		fdht_disconnect_all_servers(&g_group_array);
	}

	fdht_client_destroy();
	fprintf(stderr, "module shut down. file: "__FILE__", line: %d\n", __LINE__);
	return SUCCESS;
}
// }}}

//
// {{{ PHP_RINIT_FUNCTION
PHP_RINIT_FUNCTION(fastdht_client)
{
	return SUCCESS;
}
// }}} 

// {{{ PHP_RSHUTDOWN_FUNCTION
PHP_RSHUTDOWN_FUNCTION(fastdht_client)
{
	fprintf(stderr, "request shut down. file: "__FILE__", line: %d\n", __LINE__);
	return SUCCESS;
}
// }}}

// {{{ PHP_MINFO_FUNCTION
PHP_MINFO_FUNCTION(fastdht_client)
{
	php_info_print_table_start();
	php_info_print_table_header(2, "fastdht_client support", "enabled");
	php_info_print_table_end();

}
// }}} 
//}}}
