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

int fdht_get(GroupArray *pGroupArray, const char *pKey, const int key_len, \
		char **ppValue, int *value_len);
int fdht_set(GroupArray *pGroupArray, const char *pKey, const int key_len, \
	const char *pValue, const int value_len);
int fdht_delete(GroupArray *pGroupArray, const char *pKey, const int key_len);

#ifdef __cplusplus
}
#endif

#endif

