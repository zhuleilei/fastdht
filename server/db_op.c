#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include <fcntl.h>
#include <errno.h>
#include <db.h>
#include "logger.h"
#include "shared_func.h"
#include "db_op.h"
#include "global.h"

static void db_errcall(const DB_ENV *dbenv, const char *errpfx, const char *msg)
{
	logError("file: "__FILE__", line: %d, " \
		"db error, error info: %s", \
		__LINE__, msg);
}

int db_init(DBInfo *pDBInfo, const DBType type, const u_int64_t nCacheSize, \
	const u_int32_t page_size, const char *base_path, const char *filename)
{
#define _DB_BLOCK_BYTES   (256 * 1024 * 1024)
	int result;
	u_int32_t gb;
	u_int32_t bytes;
	int blocks;
	char *sub_dirs[] = {"tmp", "logs", "data"};
	char full_path[256];
	int i;

	pDBInfo->env = NULL;
	pDBInfo->db = NULL;
	
	for (i=0; i<sizeof(sub_dirs)/sizeof(char *); i++)
	{
		snprintf(full_path, sizeof(full_path), "%s/%s", \
			base_path, sub_dirs[i]);
		if (!fileExists(full_path))
		{
			if (mkdir(full_path, 0755) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"mkdir %s fail, " \
					"errno: %d, error info: %s", \
					__LINE__, full_path, \
					errno, strerror(errno));
				return errno != 0 ? errno : EPERM;
			}
		}
	}

	if ((result=db_env_create(&(pDBInfo->env), 0)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_env_create fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	if ((result=pDBInfo->env->set_alloc(pDBInfo->env, \
			malloc, realloc, free)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"env->set_alloc fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	pDBInfo->env->set_tmp_dir(pDBInfo->env, "tmp");
	pDBInfo->env->set_lg_dir(pDBInfo->env, "logs");
	pDBInfo->env->set_data_dir(pDBInfo->env, "data");
	pDBInfo->env->set_errcall(pDBInfo->env, db_errcall);

	gb = (u_int32_t)(nCacheSize / (1024 * 1024 * 1024));
	bytes = (u_int32_t)(nCacheSize - (u_int64_t)gb  * (1024 * 1024 * 1024));
	blocks = (int)(nCacheSize / _DB_BLOCK_BYTES);
	if (nCacheSize % _DB_BLOCK_BYTES != 0)
	{
		blocks++;
	}

	//printf("gb=%d, bytes=%d, blocks=%d\n", gb, bytes, blocks);
	if ((result=pDBInfo->env->set_cachesize(pDBInfo->env, \
			gb, bytes, blocks)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"env->set_cachesize fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	if ((result=pDBInfo->env->open(pDBInfo->env, base_path, \
		DB_CREATE | DB_INIT_MPOOL /*| DB_INIT_LOCK*/ | DB_THREAD, 0644))!=0)
	{
		logError("file: "__FILE__", line: %d, " \
			"env->open fail, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	if ((result=pDBInfo->env->set_flags(pDBInfo->env, DB_TXN_NOSYNC, 1))!=0)
	{
		logError("file: "__FILE__", line: %d, " \
			"env->set_flags fail, errno: %d, error info: %s", \
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

	if ((result=pDBInfo->db->set_pagesize(pDBInfo->db, page_size)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db->set_pagesize, errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return result;
	}

	if ((result=pDBInfo->db->open(pDBInfo->db, NULL, filename, NULL, \
		type, DB_CREATE | DB_THREAD /*| DB_AUTO_COMMIT*/, 0644)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db->open fail, errno: %d, error info: %s", \
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

int db_sync(DBInfo *pDBInfo)
{
	int result;
	if (pDBInfo->db != NULL)
	{
		if ((result=pDBInfo->db->sync(pDBInfo->db, 0)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"db_sync fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, db_strerror(result));
		}
	}
	else
	{
		result = 0;
	}

	return result;
}

int db_memp_trickle(DBInfo *pDBInfo)
{
	int result;
	int nwrotep;
	if ((result=pDBInfo->env->memp_trickle(pDBInfo->env, 60, &nwrotep)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"memp_trickle fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
	}

	logInfo("memp_trickle %d%%, real write %d%%", 60, nwrotep);
	return result;
}

int db_set(DBInfo *pDBInfo, const char *pKey, const int key_len, \
	const char *pValue, const int value_len)
{
	int result;
	DBT key;
	DBT value;

	g_server_stat.total_set_count++;

	memset(&key, 0, sizeof(key));
	memset(&value, 0, sizeof(value));

	key.data = (char *)pKey;
	key.size = key_len;

	value.data = (char *)pValue;
	value.size = value_len;

	if ((result=pDBInfo->db->put(pDBInfo->db, NULL, &key,  &value, 0)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_put fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return EFAULT;
	}

	//printf("pKey=%08X, key.data=%08X\n", (int)pKey, (int)key.data);
	//printf("pValue=%08X, value.data=%08X\n", (int)pValue,(int)value.data);
	g_server_stat.success_set_count++;
	return result;
}

static int _db_do_get(DBInfo *pDBInfo, const char *pKey, const int key_len, \
		char **ppValue, int *size)
{
	int result;
	DBT key;
	DBT value;

	memset(&key, 0, sizeof(key));
	memset(&value, 0, sizeof(value));

	key.data = (char *)pKey;
	key.size = key_len;

	if (*ppValue != NULL)
	{
		value.flags = DB_DBT_USERMEM;
		value.data = *ppValue;
		value.ulen = *size;
	}
	else
	{
		value.flags = DB_DBT_MALLOC;
	}

	if ((result=pDBInfo->db->get(pDBInfo->db, NULL, &key,  &value, 0)) != 0)
	{
		if (result == DB_NOTFOUND)
		{
			return ENOENT;
		}
		else if (result == DB_BUFFER_SMALL)
		{
			*size = value.size;
			return ENOSPC;
		}
		else
		{
			logError("file: "__FILE__", line: %d, " \
				"db_get fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, db_strerror(result));
			return EFAULT;
		}
	}

	*ppValue = value.data;
	*size = value.size;

	return result;
}

int db_get(DBInfo *pDBInfo, const char *pKey, const int key_len, \
		char **ppValue, int *size)
{
	int result;

	g_server_stat.total_get_count++;
	if ((result=_db_do_get(pDBInfo, pKey, key_len, \
		ppValue, size)) == 0)
	{
		g_server_stat.success_get_count++;
	}

	return result;
}

int db_delete(DBInfo *pDBInfo, const char *pKey, const int key_len)
{
	int result;
	DBT key;

	g_server_stat.total_delete_count++;

	memset(&key, 0, sizeof(key));
	key.data = (char *)pKey;
	key.size = key_len;

	if ((result=pDBInfo->db->del(pDBInfo->db, NULL, &key, 0)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_del fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
	}

	g_server_stat.success_delete_count++;
	return result;
}

int db_inc(DBInfo *pDBInfo, const char *pKey, const int key_len, \
	const int inc, char *pValue, int *value_len)
{
	int64_t n;
	int result;
	DBT key;
	DBT value;

	g_server_stat.total_inc_count++;

	if ((result=_db_do_get(pDBInfo, pKey, key_len, \
               	&pValue, value_len)) != 0)
	{
		if (result != ENOSPC)
		{
			return result;
		}

		n = inc;
	}
	else
	{
		pValue[*value_len] = '\0';
		n = strtoll(pValue, NULL, 10);
		n += inc;
	}

	*value_len = sprintf(pValue, INT64_PRINTF_FORMAT, n);

	memset(&key, 0, sizeof(key));
	memset(&value, 0, sizeof(value));

	key.data = (char *)pKey;
	key.size = key_len;

	value.data = (char *)pValue;
	value.size = *value_len;

	if ((result=pDBInfo->db->put(pDBInfo->db, NULL, &key,  &value, 0)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_put fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return EFAULT;
	}

	g_server_stat.success_inc_count++;
	return result;
}

int db_inc_ex(DBInfo *pDBInfo, const char *pKey, const int key_len, \
	const int inc, char *pValue, int *value_len, const int expires)
{
	int64_t n;
	int result;
	DBT key;
	DBT value;

	g_server_stat.total_inc_count++;

	if ((result=_db_do_get(pDBInfo, pKey, key_len, \
               	&pValue, value_len)) != 0)
	{
		if (result != ENOSPC)
		{
			return result;
		}

		n = inc;
	}
	else
	{
		pValue[*value_len] = '\0';
		n = strtoll(pValue+4, NULL, 10);
		n += inc;
	}

	if (expires != FDHT_EXPIRES_NONE)
	{
		int2buff(expires, pValue);
	}

	*value_len = 4 + sprintf(pValue+4, INT64_PRINTF_FORMAT, n);

	memset(&key, 0, sizeof(key));
	memset(&value, 0, sizeof(value));

	key.data = (char *)pKey;
	key.size = key_len;

	value.data = (char *)pValue;
	value.size = *value_len;

	if ((result=pDBInfo->db->put(pDBInfo->db, NULL, &key,  &value, 0)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"db_put fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, db_strerror(result));
		return EFAULT;
	}

	g_server_stat.success_inc_count++;
	return result;
}

