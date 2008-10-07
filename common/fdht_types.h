/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdht_types.h

#ifndef _FDHT_TYPES_H
#define _FDHT_TYPES_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fdht_define.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
	int sock;
	int port;
	char ip_addr[IP_ADDRESS_SIZE];
} FDHTServerInfo;

typedef struct {
	uint64_t total_set_count;
	uint64_t success_set_count;
	uint64_t total_inc_count;
	uint64_t success_inc_count;
	uint64_t total_delete_count;
	uint64_t success_delete_count;
	uint64_t total_get_count;
	uint64_t success_get_count;
} FDHTServerStat;

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

#ifdef __cplusplus
}
#endif

#endif

