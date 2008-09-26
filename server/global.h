/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//global.h

#ifndef _GLOBAL_H
#define _GLOBAL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include "fdht_define.h"

#ifdef __cplusplus
extern "C" {
#endif

extern bool g_continue_flag;

extern int g_server_port;
extern int g_max_connections;
extern int g_max_threads;
extern int g_min_buff_size;

extern pthread_mutex_t g_storage_thread_lock;
extern int g_thread_count;

extern struct timeval g_network_tv;

extern int g_conn_count;
extern int g_recv_count;
extern int g_send_count;

extern int g_allow_ip_count;  /* -1 means match any ip address */
extern in_addr_t *g_allow_ip_addrs;  /* sorted array, asc order */

#ifdef __cplusplus
}
#endif

#endif
