#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include <fcntl.h>
#include <errno.h>
#include "logger.h"
#include "shared_func.h"
#include "key_op.h"
#include "global.h"
#include "func.h"

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

int key_add(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo)
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

int key_del(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo)
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

int key_batch_add(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
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
		return key_add(pHandle, pKeyInfo);
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

			pSubKey++;
		}
		else if (compare == 0)
		{
			memcpy(p, pSubKey->szKey, pSubKey->key_len);
			p += pSubKey->key_len;

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
		*p++ = FDHT_KEY_LIST_SEPERATOR;
		memcpy(p, pSubKey->szKey, pSubKey->key_len);
		p += pSubKey->key_len;

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

int key_batch_del(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
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
		return key_del(pHandle, pKeyInfo);
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

