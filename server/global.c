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
#include "logger.h"
#include "global.h"

bool g_continue_flag = true;

int g_server_port = FDFS_STORAGE_SERVER_DEF_PORT;
int g_max_connections = 10240;  //FDFS_DEF_MAX_CONNECTONS;
int g_max_threads = 5;
int g_min_buff_size = 4 * 1024;

pthread_mutex_t g_storage_thread_lock;
int g_thread_count = 0;

struct timeval g_network_tv = {30, 0};

int g_conn_count = 0;
int g_recv_count = 0;
int g_send_count = 0;

int g_allow_ip_count = 0;  /* -1 means match any ip address */
in_addr_t *g_allow_ip_addrs = NULL;  /* sorted array, asc order */

