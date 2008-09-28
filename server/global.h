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
#include "fdht_global.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FDHT_MAX_LOCAL_IP_ADDRS     4
#define DEFAULT_SYNC_WAIT_MSEC    100

extern bool g_continue_flag;

extern int g_server_port;
extern int g_max_connections;
extern int g_max_threads;
extern int g_max_pkg_size;
extern int g_min_buff_size;

extern pthread_mutex_t g_storage_thread_lock;
extern int g_thread_count;
extern int g_sync_wait_usec;

extern struct timeval g_network_tv;

extern int g_conn_count;
extern int g_recv_count;
extern int g_send_count;

extern int g_allow_ip_count;  /* -1 means match any ip address */
extern in_addr_t *g_allow_ip_addrs;  /* sorted array, asc order */

extern int g_local_host_ip_count;
extern char g_local_host_ip_addrs[FDHT_MAX_LOCAL_IP_ADDRS * \
				IP_ADDRESS_SIZE];

void load_local_host_ip_addrs();
bool is_local_host_ip(const char *client_ip);
int insert_into_local_host_ip(const char *client_ip);
void print_local_host_ip_addrs();

#ifdef __cplusplus
}
#endif

#endif
