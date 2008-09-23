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
#include "fdfs_define.h"

#ifdef __cplusplus
extern "C" {
#endif

int fdht_service_init();
void fdht_service_destroy();

int fdht_load_from_conf_file(const char *filename, char *bind_addr, \
		const int addr_size, int **group_ids, int *group_count);

#ifdef __cplusplus
}
#endif

#endif

