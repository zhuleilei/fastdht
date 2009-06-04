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
#include "sockopt.h"
#include "global.h"

bool g_continue_flag = true;

int g_server_port = FDHT_SERVER_DEFAULT_PORT;
int g_max_connections = DEFAULT_MAX_CONNECTONS;
int g_max_threads = 5;
int g_max_pkg_size = FDHT_MAX_PKG_SIZE;
int g_min_buff_size = FDHT_MIN_BUFF_SIZE;
int g_heart_beat_interval = DEFAULT_NETWORK_TIMEOUT / 2;
bool g_write_to_binlog_flag = true;

int g_thread_count = 0;
int g_sync_wait_usec = DEFAULT_SYNC_WAIT_MSEC;
int g_sync_log_buff_interval = SYNC_LOG_BUFF_DEF_INTERVAL;
int g_sync_binlog_buff_interval = SYNC_BINLOG_BUFF_DEF_INTERVAL;
TimeInfo g_sync_db_time_base = {TIME_NONE, TIME_NONE};
int g_sync_db_interval = DEFAULT_SYNC_DB_INVERVAL;
TimeInfo g_clear_expired_time_base = {TIME_NONE, TIME_NONE};
int g_clear_expired_interval = DEFAULT_CLEAR_EXPIRED_INVERVAL;
int g_db_dead_lock_detect_interval = DEFAULT_DB_DEAD_LOCK_DETECT_INVERVAL;
TimeInfo g_compress_binlog_time_base = {TIME_NONE, TIME_NONE};
int g_compress_binlog_interval = COMPRESS_BINLOG_DEF_INTERVAL;

struct timeval g_network_tv = {DEFAULT_NETWORK_TIMEOUT, 0};

FDHTServerStat g_server_stat;

int g_group_count = 0;
FDHTGroupServer *g_group_servers = NULL;
int g_group_server_count = 0;

int g_server_join_time = 0;
bool g_sync_old_done = false;
char g_sync_src_ip_addr[IP_ADDRESS_SIZE] = {0};
int g_sync_src_port = 0;
int g_sync_until_timestamp = 0;
int g_sync_done_timestamp = 0;

int g_allow_ip_count = 0;  /* -1 means match any ip address */
in_addr_t *g_allow_ip_addrs = NULL;  /* sorted array, asc order */

int g_local_host_ip_count = 0;
char g_local_host_ip_addrs[FDHT_MAX_LOCAL_IP_ADDRS * \
				IP_ADDRESS_SIZE];
time_t g_server_start_time = 0;

bool is_local_host_ip(const char *client_ip)
{
	char *p;
	char *pEnd;

	pEnd = g_local_host_ip_addrs + \
		IP_ADDRESS_SIZE * g_local_host_ip_count;
	for (p=g_local_host_ip_addrs; p<pEnd; p+=IP_ADDRESS_SIZE)
	{
		if (strcmp(client_ip, p) == 0)
		{
			return true;
		}
	}

	return false;
}

int insert_into_local_host_ip(const char *client_ip)
{
	if (is_local_host_ip(client_ip))
	{
		return 0;
	}

	if (g_local_host_ip_count >= FDHT_MAX_LOCAL_IP_ADDRS)
	{
		return -1;
	}

	strcpy(g_local_host_ip_addrs + \
		IP_ADDRESS_SIZE * g_local_host_ip_count, \
		client_ip);
	g_local_host_ip_count++;
	return 1;
}

void load_local_host_ip_addrs()
{
	char ip_addresses[FDHT_MAX_LOCAL_IP_ADDRS][IP_ADDRESS_SIZE];
	int count;
	int k;

	insert_into_local_host_ip("127.0.0.1");
	if (gethostaddrs(ip_addresses, FDHT_MAX_LOCAL_IP_ADDRS, &count) != 0)
	{
		return;
	}

	for (k=0; k<count; k++)
	{
		insert_into_local_host_ip(ip_addresses[k]);
	}
}

void print_local_host_ip_addrs()
{
	char *p;
	char *pEnd;

	printf("local_host_ip_count=%d\n", g_local_host_ip_count);
	pEnd = g_local_host_ip_addrs + \
		IP_ADDRESS_SIZE * g_local_host_ip_count;
	for (p=g_local_host_ip_addrs; p<pEnd; p+=IP_ADDRESS_SIZE)
	{
		printf("%d. %s\n", (int)((p-g_local_host_ip_addrs)/ \
				IP_ADDRESS_SIZE)+1, p);
	}

	printf("\n");
}

