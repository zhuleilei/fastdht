/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//func.h

#ifndef _FUNC_H
#define _FUNC_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fdht_define.h"
#include "db_op.h"

#ifdef __cplusplus
extern "C" {
#endif

extern DBInfo **g_db_list;
extern int g_db_count;

typedef char * (*get_filename_func)(const void *pArg, \
			char *full_filename);

int fdht_write_to_fd(int fd, get_filename_func filename_func, \
		const void *pArg, const char *buff, const int len);

int fdht_write_to_stat_file();
int fdht_terminate();

int fdht_func_init(const char *filename, char *bind_addr, const int addr_size);
void fdht_func_destroy();

void fdht_sync_dbs(void *args);
void fdht_memp_trickle_dbs(void *args);
void fdht_memp_sync_dbs(void *args);

int group_cmp_by_ip_and_port(const void *p1, const void *p2);

#ifdef __cplusplus
}
#endif

#endif

