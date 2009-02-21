/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <netdb.h>
#include <unistd.h>
#include <errno.h>
#include "fdht_global.h"

int g_network_timeout = DEFAULT_NETWORK_TIMEOUT;
char g_base_path[MAX_PATH_SIZE];
Version g_version = {1, 6};

