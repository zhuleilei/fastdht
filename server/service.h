/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//service.h

#ifndef _SERVICE_H
#define _SERVICE_H

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

int fdht_service_init(const char *filename, char *bind_addr, \
		const int addr_size);
void fdht_service_destroy();

#ifdef __cplusplus
}
#endif

#endif

