/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdht_client.h

#ifndef _FDHT_CLIENT_H
#define _FDHT_CLIENT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fdht_define.h"
#include "fdht_types.h"
#include "fdht_proto.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
	FDHTServerInfo *servers;
	int count;  //server count
	int read_index;  //current read index for roundrobin
} ServerArray;

typedef struct
{
	ServerArray *groups;
	int count;  //group count
} GroupArray;

extern GroupArray g_group_array;

int fdht_client_init(const char *filename);
void fdht_client_destroy();

int fdht_get(const char *pKey, const int key_len, \
		char **ppValue, int *value_len);

int fdht_set(const char *pKey, const int key_len, \
	const char *pValue, const int value_len);

int fdht_inc(const char *pKey, const int key_len, \
		const int increase);

int fdht_delete(const char *pKey, const int key_len);

#ifdef __cplusplus
}
#endif

#endif

