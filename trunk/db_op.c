#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <limits.h>
#include <fcntl.h>
#include <db.h>
#include "logger.h"
#include "db_op.h"

int db_init(DBInfo *pDBInfo, const DBType type, const u_int32_t nCacheSize, \
	const char *base_path, const char *filename)
{
	int result;

	pDBInfo->env = NULL;
	pDBInfo->db = NULL;

	if ((result=db_env_create(&(pDBInfo->env), 0)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_env_create fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	pDBInfo->env->set_tmp_dir(pDBInfo->env, "tmp");
	pDBInfo->env->set_lg_dir(pDBInfo->env, "logs");
	pDBInfo->env->set_data_dir(pDBInfo->env, "data");
	if ((result=pDBInfo->env->open(pDBInfo->env, base_path, \
		DB_CREATE | DB_INIT_MPOOL | DB_THREAD, 0644)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_env_open fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	if ((result=db_create(&(pDBInfo->db), pDBInfo->env, 0)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_create fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	if ((result=pDBInfo->db->open(pDBInfo->db, NULL, filename, NULL, \
		type, DB_CREATE | DB_THREAD, 0644)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_open fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	return 0;
}

int db_destroy(DBInfo *pDBInfo)
{
	int result;
	if (pDBInfo->db != NULL)
	{
		if ((result=pDBInfo->db->close(pDBInfo->db, 0)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"db_close fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, db_strerror(result));
		}
		pDBInfo->db = NULL;
	}

	if (pDBInfo->env != NULL)
	{
		if ((result=pDBInfo->env->close(pDBInfo->env, 0)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"db_env_close fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, db_strerror(result));
		}
		pDBInfo->env = NULL;
	}

	return 0;
}

int db_set(DBInfo *pDBInfo, const char *pKey, const int key_len, \
	const char *pValue, const int value_len)
{
	int result;
	DBT key;
	DBT value;

	memset(&key, 0, sizeof(key));
	memset(&value, 0, sizeof(value));

	key.flags = DB_DBT_USERMEM;
	key.data = (char *)pKey;
	key.size = key_len;

	value.flags = DB_DBT_USERMEM;
	value.data = (char *)pValue;
	value.size = value_len;

	if ((result=pDBInfo->db->put(pDBInfo->db, NULL, &key,  &value, 0)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_put fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
	}

	return result;
}

int db_get(DBInfo *pDBInfo, const char *pKey, const int key_len, \
		char **ppValue, int *size)
{
	int result;
	DBT key;
	DBT value;

	memset(&key, 0, sizeof(key));
	memset(&value, 0, sizeof(value));

	key.flags = DB_DBT_USERMEM;
	key.data = (char *)pKey;
	key.size = key_len;

	if (*ppValue != NULL)
	{
		value.flags = DB_DBT_USERMEM;
		value.data = *ppValue;
		value.ulen = *size;
	}

	if ((result=pDBInfo->db->get(pDBInfo->db, NULL, &key,  &value, 0)) != 0)
	{
		if (result != DB_NOTFOUND)
		{
			logError("file: "__FILE__", line: %d, " \
				"db_get fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, db_strerror(result));
		}
	}
	else
	{
		*ppValue = value.data;
		*size = value.size;
	}

	return result;
}

