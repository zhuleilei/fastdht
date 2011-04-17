#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include "logger.h"
#include "shared_func.h"
#include "key_op.h"
#include "global.h"
#include "func.h"

bool g_store_key_list = false;
static pthread_mutex_t *locks;
static int lock_count = 0;

int key_init()
{
	pthread_mutex_t *pLock;
	pthread_mutex_t *lock_end;
	int result;

	if (!g_store_key_list)
	{
		return 0;
	}

	lock_count = g_thread_count;
	if (lock_count % 2 == 0)
	{
		lock_count += 1;
	}

	locks = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t) * lock_count);
	if (locks == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, (int)sizeof(pthread_mutex_t) * lock_count, \
			errno, STRERROR(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	lock_end = locks + lock_count;
	for (pLock=locks; pLock<lock_end; pLock++)
	{
		if ((result=pthread_mutex_init(pLock, NULL)) != 0)
		{
			return result;
		}
	}

	return 0;
}

int key_destroy()
{
	pthread_mutex_t *pLock;
	pthread_mutex_t *lock_end;
	if (!g_store_key_list)
	{
		return 0;
	}

	if (locks == NULL)
	{
		return 0;
	}

	lock_end = locks + lock_count;
	for (pLock=locks; pLock<lock_end; pLock++)
	{
		pthread_mutex_destroy(pLock);
	}

	free(locks);
	locks = NULL;
	return 0;
}

int key_get(StoreHandle *pHandle, const char *full_key, \
		const int full_key_len, char *key_list, int *value_len, \
		char **key_array, int *key_count)
{
	int result;

	*value_len = FDHT_KEY_LIST_MAX_SIZE - 1;
	result = g_func_get(pHandle, full_key, full_key_len, \
				&key_list, value_len);
	if (result == ENOENT)
	{
		*value_len = 0;
	}
	else if (result != 0)
	{
		return result;
	}

	if (*value_len == 0)
	{
		*key_count = 0;
		return 0;
	}

	*(key_list + *value_len) = '\0';

	*key_count = splitEx(key_list, FDHT_KEY_LIST_SEPERATOR, \
				key_array, *key_count);
	return 0;
}

static int key_compare(const void *p1, const void *p2)
{
	return strcmp(*((const char **)p1), *((const char **)p2));
}

static int key_do_add(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo)
{
	char *p;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	char old_key_list[FDHT_KEY_LIST_MAX_SIZE];
	char new_key_list[FDHT_KEY_LIST_MAX_SIZE];
	char *key_array[FDHT_KEY_LIST_MAX_COUNT];
	char **pFound;
	int full_key_len;
	int key_len;
	int value_len;
	int key_count;
	int result;
	int i, k;

	FDHT_PACK_LIST_KEY((*pKeyInfo), full_key, full_key_len, p)

	key_count = FDHT_KEY_LIST_MAX_COUNT;
	if ((result=key_get(pHandle, full_key, \
		full_key_len, old_key_list, &value_len, \
		key_array, &key_count)) != 0)
	{
		return result;
	}

	pFound = bsearch(pKeyInfo->szKey, key_array, key_count, \
			sizeof(char *), key_compare);
	if (pFound != NULL)
	{
		return 0;
	}

	if (value_len + 1 + pKeyInfo->key_len >= FDHT_KEY_LIST_MAX_SIZE)
	{
		return ENOSPC;
	}

	p = new_key_list;
	for (i=0; i<key_count; i++)
	{
		if (strcmp(pKeyInfo->szKey, key_array[i]) < 0)
		{
			break;
		}

		*p++ = FDHT_KEY_LIST_SEPERATOR;
		key_len = strlen(key_array[i]);
		memcpy(p, key_array[i], key_len);
		p += key_len;
	}

	*p++ = FDHT_KEY_LIST_SEPERATOR;
	memcpy(p, pKeyInfo->szKey, pKeyInfo->key_len);
	p += pKeyInfo->key_len;

	for (k=i; k<key_count; k++)
	{
		*p++ = FDHT_KEY_LIST_SEPERATOR;
		key_len = strlen(key_array[k]);
		memcpy(p, key_array[k], key_len);
		p += key_len;
	}

	value_len = (p - new_key_list) - 1;
	return g_func_set(pHandle, full_key, full_key_len, \
			new_key_list + 1, value_len);
}

static int key_do_del(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo)
{
	char *p;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	char old_key_list[FDHT_KEY_LIST_MAX_SIZE];
	char new_key_list[FDHT_KEY_LIST_MAX_SIZE];
	char *key_array[FDHT_KEY_LIST_MAX_COUNT];
	char **pFound;
	int full_key_len;
	int key_len;
	int value_len;
	int key_count;
	int index;
	int result;
	int i;

	FDHT_PACK_LIST_KEY((*pKeyInfo), full_key, full_key_len, p)

	key_count = FDHT_KEY_LIST_MAX_COUNT;
	if ((result=key_get(pHandle, full_key, \
		full_key_len, old_key_list, &value_len, \
		key_array, &key_count)) != 0)
	{
		return result;
	}

	pFound = bsearch(pKeyInfo->szKey, key_array, key_count, \
			sizeof(char *), key_compare);
	if (pFound == NULL)
	{
		logWarning("file: "__FILE__", line: %d, " \
			"namespace: %s, object id: %s, key: %s not exist!", \
			__LINE__, pKeyInfo->szNameSpace, \
			pKeyInfo->szObjectId, pKeyInfo->szKey);
		return 0;
	}

	index = pFound - key_array;
	p = new_key_list;
	for (i=0; i<index; i++)
	{
		*p++ = FDHT_KEY_LIST_SEPERATOR;
		key_len = strlen(key_array[i]);
		memcpy(p, key_array[i], key_len);
		p += key_len;
	}

	for (i=index+1; i<key_count; i++)
	{
		*p++ = FDHT_KEY_LIST_SEPERATOR;
		key_len = strlen(key_array[i]);
		memcpy(p, key_array[i], key_len);
		p += key_len;
	}

	value_len = (p - new_key_list) - 1;
	return g_func_set(pHandle, full_key, full_key_len, \
			new_key_list + 1, value_len);
}

static int key_batch_do_add(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
	FDHTSubKey *subKeys, const int sub_key_count)
{
	char *p;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	char old_key_list[FDHT_KEY_LIST_MAX_SIZE];
	char new_key_list[FDHT_KEY_LIST_MAX_SIZE];
	char *key_array[FDHT_KEY_LIST_MAX_COUNT];
	char **ppKey;
	char **ppKeyEnd;
	FDHTSubKey *pSubKey;
	FDHTSubKey *pSubEnd;
	char *pPreviousKey;
	int full_key_len;
	int key_len;
	int value_len;
	int total_len;
	int key_count;
	int result;
	int compare;

	if (sub_key_count == 0)
	{
		return 0;
	}
	if (sub_key_count == 1)
	{
		pKeyInfo->key_len = subKeys->key_len;
		memcpy(pKeyInfo->szKey, subKeys->szKey, subKeys->key_len);
		*(pKeyInfo->szKey + pKeyInfo->key_len) = '\0';
		return key_do_add(pHandle, pKeyInfo);
	}

	FDHT_PACK_LIST_KEY((*pKeyInfo), full_key, full_key_len, p)

	key_count = FDHT_KEY_LIST_MAX_COUNT;
	if ((result=key_get(pHandle, full_key, \
		full_key_len, old_key_list, &value_len, \
		key_array, &key_count)) != 0)
	{
		return result;
	}

	total_len = 0;
	pSubEnd = subKeys + sub_key_count;
	for (pSubKey=subKeys; pSubKey<pSubEnd; pSubKey++)
	{
		total_len += 1 + pSubKey->key_len;
	}

	if (value_len + total_len >= FDHT_KEY_LIST_MAX_SIZE)
	{
		return ENOSPC;
	}

	p = new_key_list;
	pSubKey = subKeys;
	ppKey = key_array;
	ppKeyEnd = key_array + key_count;
	while (pSubKey < pSubEnd && ppKey < ppKeyEnd)
	{
		*p++ = FDHT_KEY_LIST_SEPERATOR;
		compare = strcmp(pSubKey->szKey, *ppKey);
		if (compare < 0)
		{
			memcpy(p, pSubKey->szKey, pSubKey->key_len);
			p += pSubKey->key_len;
		}
		else if (compare == 0)
		{
			memcpy(p, pSubKey->szKey, pSubKey->key_len);
			p += pSubKey->key_len;

			ppKey++;
		}
		else
		{
			key_len = strlen(*ppKey);
			memcpy(p, *ppKey, key_len);
			p += key_len;

			ppKey++;
			continue;
		}

		pPreviousKey = pSubKey->szKey;
		pSubKey++;
		while (pSubKey < pSubEnd)
		{
			if (strcmp(pSubKey->szKey, pPreviousKey) != 0)
			{
				break;
			}

			pSubKey++;
		}
	}

	while (pSubKey < pSubEnd)
	{
		*p++ = FDHT_KEY_LIST_SEPERATOR;
		memcpy(p, pSubKey->szKey, pSubKey->key_len);
		p += pSubKey->key_len;

		pPreviousKey = pSubKey->szKey;
		pSubKey++;
		while (pSubKey < pSubEnd)
		{
			if (strcmp(pSubKey->szKey, pPreviousKey) != 0)
			{
				break;
			}

			pSubKey++;
		}
	}

	while (ppKey < ppKeyEnd)
	{
		*p++ = FDHT_KEY_LIST_SEPERATOR;
		key_len = strlen(*ppKey);
		memcpy(p, *ppKey, key_len);
		p += key_len;

		ppKey++;
	}

	value_len = (p - new_key_list) - 1;
	return g_func_set(pHandle, full_key, full_key_len, \
			new_key_list + 1, value_len);
}

static int key_batch_do_del(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
	FDHTSubKey *subKeys, const int sub_key_count)
{
	char *p;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	char old_key_list[FDHT_KEY_LIST_MAX_SIZE];
	char new_key_list[FDHT_KEY_LIST_MAX_SIZE];
	char *key_array[FDHT_KEY_LIST_MAX_COUNT];
	char **ppKey;
	char **ppKeyEnd;
	FDHTSubKey *pSubKey;
	FDHTSubKey *pSubEnd;
	int full_key_len;
	int key_len;
	int value_len;
	int key_count;
	int result;
	int compare;

	if (sub_key_count == 0)
	{
		return 0;
	}
	if (sub_key_count == 1)
	{
		pKeyInfo->key_len = subKeys->key_len;
		memcpy(pKeyInfo->szKey, subKeys->szKey, subKeys->key_len);
		*(pKeyInfo->szKey + pKeyInfo->key_len) = '\0';
		return key_do_del(pHandle, pKeyInfo);
	}

	FDHT_PACK_LIST_KEY((*pKeyInfo), full_key, full_key_len, p)

	key_count = FDHT_KEY_LIST_MAX_COUNT;
	if ((result=key_get(pHandle, full_key, \
		full_key_len, old_key_list, &value_len, \
		key_array, &key_count)) != 0)
	{
		return result;
	}

	p = new_key_list;
	pSubKey = subKeys;
	ppKey = key_array;
	pSubEnd = subKeys + sub_key_count;
	ppKeyEnd = key_array + key_count;
	while (pSubKey < pSubEnd && ppKey < ppKeyEnd)
	{
		*p++ = FDHT_KEY_LIST_SEPERATOR;
		compare = strcmp(pSubKey->szKey, *ppKey);
		if (compare < 0)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"namespace: %s, object id: %s, " \
				"key: %s not exist!", \
				__LINE__, pKeyInfo->szNameSpace, \
				pKeyInfo->szObjectId, pSubKey->szKey);
			pSubKey++;
		}
		else if (compare == 0)
		{
			pSubKey++;
			ppKey++;
		}
		else
		{
			key_len = strlen(*ppKey);
			memcpy(p, *ppKey, key_len);
			p += key_len;

			ppKey++;
		}
	}

	while (pSubKey < pSubEnd)
	{
		logWarning("file: "__FILE__", line: %d, " \
			"namespace: %s, object id: %s, " \
			"key: %s not exist!", \
			__LINE__, pKeyInfo->szNameSpace, \
			pKeyInfo->szObjectId, pSubKey->szKey);

		pSubKey++;
	}

	while (ppKey < ppKeyEnd)
	{
		*p++ = FDHT_KEY_LIST_SEPERATOR;
		key_len = strlen(*ppKey);
		memcpy(p, *ppKey, key_len);
		p += key_len;

		ppKey++;
	}

	value_len = (p - new_key_list) - 1;
	return g_func_set(pHandle, full_key, full_key_len, \
			new_key_list + 1, value_len);
}

int key_add(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
		const int key_hash_code)
{
	int result;
	int index;

	if (pKeyInfo->namespace_len <= 0 || pKeyInfo->obj_id_len <= 0)
	{
		return 0;
	}

	index = ((const unsigned int)key_hash_code) % lock_count;

	pthread_mutex_lock(locks + index);
	result = key_do_add(pHandle, pKeyInfo);
	pthread_mutex_unlock(locks + index);

	return result;
}

int key_del(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
		const int key_hash_code)
{
	int result;
	int index;

	if (pKeyInfo->namespace_len <= 0 || pKeyInfo->obj_id_len <= 0)
	{
		return 0;
	}

	index = ((const unsigned int)key_hash_code) % lock_count;

	pthread_mutex_lock(locks + index);
	result = key_do_del(pHandle, pKeyInfo);
	pthread_mutex_unlock(locks + index);

	return result;
}

int key_batch_add(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
	const int key_hash_code, FDHTSubKey *subKeys, const int sub_key_count)
{
	int result;
	int index;

	if (pKeyInfo->namespace_len <= 0 || pKeyInfo->obj_id_len <= 0)
	{
		return 0;
	}

	index = ((const unsigned int)key_hash_code) % lock_count;

	pthread_mutex_lock(locks + index);
	result = key_batch_do_add(pHandle, pKeyInfo, subKeys, sub_key_count);
	pthread_mutex_unlock(locks + index);

	return result;
}

int key_batch_del(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
	const int key_hash_code, FDHTSubKey *subKeys, const int sub_key_count)
{
	int result;
	int index;

	if (pKeyInfo->namespace_len <= 0 || pKeyInfo->obj_id_len <= 0)
	{
		return 0;
	}

	index = ((const unsigned int)key_hash_code) % lock_count;

	pthread_mutex_lock(locks + index);
	result = key_batch_do_del(pHandle, pKeyInfo, subKeys, sub_key_count);
	pthread_mutex_unlock(locks + index);

	return result;
}

