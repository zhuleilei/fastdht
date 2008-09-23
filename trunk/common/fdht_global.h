/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdht_global.h

#ifndef _FDHT_GLOBAL_H
#define _FDHT_GLOBAL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include "fdfs_define.h"

typedef struct
{
	char major;
	char minor;
} Version;

#ifdef __cplusplus
extern "C" {
#endif

extern int g_network_timeout;
extern char g_base_path[MAX_PATH_SIZE];
extern Version g_version;

#ifdef __cplusplus
}
#endif

#endif

