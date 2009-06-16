//mpool_op.c

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "logger.h"
#include "mpool_op.h"
#include "global.h"
#include "shared_func.h"

HashArray *g_hash_array = NULL;

int mp_init(StoreHandle **ppHandle, const u_int64_t nCacheSize)
{
	int result;
	if (g_hash_array != NULL)
	{
		*ppHandle = g_hash_array;
		return 0;
	}

	g_hash_array = (HashArray *)malloc(sizeof(HashArray));
	if (g_hash_array == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, sizeof(HashArray), errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	if ((result=hash_init_ex(g_hash_array, PJWHash, \
		g_mpool_init_capacity, g_mpool_load_factor, nCacheSize)) != 0)
	{
		return result;
	}

	*ppHandle = g_hash_array;
	return 0;
}

int mp_destroy_instance(StoreHandle **ppHandle)
{
	return 0;
}

int mp_destroy()
{
	return 0;
}

int mp_memp_trickle(int *nwrotep)
{
	*nwrotep = 0;
	return 0;
}

int mp_get(StoreHandle *pHandle, const char *pKey, const int key_len, \
		char **ppValue, int *size)
{
	HashData *hash_data;

	g_server_stat.total_get_count++;
	hash_data = hash_find_ex(g_hash_array, pKey, key_len);
	if (hash_data == NULL)
	{
		return ENOENT;
	}

	if (*ppValue != NULL)
	{
		if (*size < hash_data->value_len)
		{
			*size = hash_data->value_len;
			return ENOSPC;
		}

		*size = hash_data->value_len;
		memcpy(*ppValue, HASH_VALUE(hash_data), hash_data->value_len);
		g_server_stat.success_get_count++;
		return 0;
	}

	*size = hash_data->value_len;
	*ppValue = (char *)malloc(hash_data->value_len);
	if (*ppValue == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, hash_data->value_len, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	memcpy(*ppValue, HASH_VALUE(hash_data), hash_data->value_len);
	g_server_stat.success_get_count++;
	return 0;
}

int mp_do_set(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const char *pValue, const int value_len)
{
	int result;
	int i;
	
	for (i=0; i<2; i++)
	{
		result = hash_insert_ex(pHandle, pKey, key_len, \
				(void *)pValue, value_len);
		if (result < 0)
		{
			result *= -1;
			if (result == ENOSPC)
			{
				if (mp_clear_expired_keys(pHandle) > 0)
				{
					continue;
				}
			}

			return result;
		}
		else
		{
			return 0;
		}
	}

	return result;
}

int mp_set(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const char *pValue, const int value_len)
{
	int result;

	g_server_stat.total_set_count++;
	result = mp_do_set(g_hash_array, pKey, key_len, pValue, value_len);
	if (result == 0)
	{
		g_server_stat.success_set_count++;
	}
	return result;
}

int mp_partial_set(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const char *pValue, const int offset, const int value_len)
{
	HashData *hash_data;
	int result;
	char *pNewBuff;

	hash_data = hash_find_ex(g_hash_array, pKey, key_len);
	if (hash_data == NULL)
	{
		if (offset != 0)
		{
			return ENOENT;
		}

		return mp_do_set(g_hash_array, pKey,key_len, pValue,value_len);
	}

	if (offset < 0 || offset >= hash_data->value_len)
	{
		return EINVAL;
	}

	if (offset + value_len <= hash_data->value_len)
	{
		memcpy(HASH_VALUE(hash_data)+offset, pValue, value_len);
		return 0;
	}

	pNewBuff = (char *)malloc(offset + value_len);
	if (pNewBuff == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, offset + value_len, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	if (offset > 0)
	{
		memcpy(pNewBuff, HASH_VALUE(hash_data), offset);
	}
	memcpy(pNewBuff+offset, pValue, value_len);
	result = mp_do_set(g_hash_array, pKey, key_len, \
			pNewBuff, offset + value_len);
	free(pNewBuff);

	return result;
}

int mp_delete(StoreHandle *pHandle, const char *pKey, const int key_len)
{
	int result;

	g_server_stat.total_delete_count++;
	result = hash_delete(g_hash_array, pKey, key_len);
	if (result == 0)
	{
		g_server_stat.success_delete_count++;
	}

	return result;
}

int mp_inc(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const int inc, char *pValue, int *value_len)
{
	HashData *hash_data;
	int result;
	int64_t n;

	g_server_stat.total_inc_count++;

	hash_data = hash_find_ex(g_hash_array, pKey, key_len);
	if (hash_data == NULL)
	{
		n = inc;
	}
	else
	{
		if (hash_data->value_len >= *value_len)
		{
			return ENOSPC;
		}

		memcpy(pValue, HASH_VALUE(hash_data), hash_data->value_len);
		pValue[hash_data->value_len] = '\0';
		n = strtoll(pValue, NULL, 10);
		n += inc;
	}

	*value_len = sprintf(pValue, INT64_PRINTF_FORMAT, n);

	result = mp_do_set(g_hash_array, pKey, key_len, pValue, *value_len);
	if (result == 0)
	{
		g_server_stat.success_inc_count++;
	}
	return result;
}

int mp_inc_ex(StoreHandle *pHandle, const char *pKey, const int key_len, \
	const int inc, char *pValue, int *value_len, const int expires)
{
	HashData *hash_data;
	int64_t n;
	int result;
	int old_expires;

	g_server_stat.total_inc_count++;

	hash_data = hash_find_ex(g_hash_array, pKey, key_len);
	if (hash_data == NULL)
	{
		n = inc;
	}
	else
	{
		old_expires = buff2int(HASH_VALUE(hash_data));
		if (old_expires != FDHT_EXPIRES_NEVER && \
			old_expires < time(NULL)) //expired
		{
			n = inc;
		}
		else
		{
			if (hash_data->value_len >= *value_len)
			{
				n = inc;
			}
			else
			{
				memcpy(pValue, HASH_VALUE(hash_data), \
					hash_data->value_len);
				pValue[hash_data->value_len] = '\0';
				n = strtoll(pValue + 4, NULL, 10);
				n += inc;
			}
		}
	}

	int2buff(expires, pValue);
	*value_len = 4 + sprintf(pValue+4, INT64_PRINTF_FORMAT, n);

	result = mp_do_set(g_hash_array, pKey, key_len, pValue, *value_len);
	if (result == 0)
	{
		g_server_stat.success_inc_count++;
	}

	return result;
}

int mp_clear_expired_keys(void *arg)
{
	HashData **ppBucket;
	HashData **bucket_end;
	HashData *hash_data;
	HashData *pDeleted;
	HashData *previous;
	time_t current_time;
	int expires;
	struct timeval tv_start;
	struct timeval tv_end;
	int old_item_count;
	static time_t last_clear_time = 0;
	static bool clearing = false;

	if (clearing)
	{
		logInfo("file: "__FILE__", line: %d, " \
			"clear proccess already running", __LINE__);
		return 0;
	}

	if (gettimeofday(&tv_start, NULL) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call gettimeofday fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return -1;
	}

	current_time = tv_start.tv_sec;
	if (current_time - last_clear_time < g_mpool_clear_min_interval)
	{
		return 0;
	}

	if (clearing)
	{
		logInfo("file: "__FILE__", line: %d, " \
			"clear proccess already running", __LINE__);
		return 0;
	}
	clearing = true;

	old_item_count = g_hash_array->item_count;
	bucket_end = g_hash_array->buckets + (*g_hash_array->capacity);
	for (ppBucket=g_hash_array->buckets; ppBucket<bucket_end; ppBucket++)
	{
		if (*ppBucket == NULL)
		{
			continue;
		}

		hash_data = *ppBucket;
		previous = NULL;
		do
		{
			expires = buff2int(HASH_VALUE(hash_data));
			if (expires == FDHT_EXPIRES_NEVER || \
				expires > current_time)
			{
				if (previous == NULL)
				{
					*ppBucket = hash_data;
				}
				else
				{
					previous->next = hash_data;
				}

				previous = hash_data;
				hash_data = hash_data->next;
			}
			else
			{
				pDeleted = hash_data;
				hash_data = hash_data->next;

				FREE_HASH_DATA(g_hash_array, pDeleted)
			}

		} while (hash_data != NULL);

		if (previous == NULL)
		{
			*ppBucket = NULL;
		}
		else
		{
			previous->next = NULL;
		}
	}

	gettimeofday(&tv_end, NULL);

	logInfo("clear expired keys, total key count: %d, " \
		"expired key count: %d, time used: %dms, mpool free bytes: " \
		INT64_PRINTF_FORMAT" bytes", \
		old_item_count, old_item_count - g_hash_array->item_count, \
		(int)((tv_end.tv_sec - tv_start.tv_sec) * 1000 + \
		(tv_end.tv_usec - tv_start.tv_usec) / 1000), \
		g_hash_array->max_bytes - g_hash_array->bytes_used);

	clearing = false;
	last_clear_time = tv_end.tv_sec;

	return old_item_count - g_hash_array->item_count;
}

