/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//key_op.h

#ifndef _KEY_OP_H
#define _KEY_OP_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <db.h>
#include "store.h"
#include "fdht_define.h"
#include "fdht_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int key_get(StoreHandle *pHandle, const char *full_key, \
		const int full_key_len, char *key_list, int *value_len, \
		char **key_array, int *key_count);

int key_add(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo);
int key_del(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo);

int key_batch_add(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
	FDHTSubKey *subKeys, const int sub_key_count);

int key_batch_del(StoreHandle *pHandle, FDHTKeyInfo *pKeyInfo, \
	FDHTSubKey *subKeys, const int sub_key_count);

#ifdef __cplusplus
}
#endif

#endif

