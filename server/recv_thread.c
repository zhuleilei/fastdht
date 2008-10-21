/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include <event.h>
#include "shared_func.h"
#include "logger.h"
#include "fdht_global.h"
#include "global.h"
#include "ini_file_reader.h"
#include "sockopt.h"
#include "fdht_proto.h"
#include "task_queue.h"
#include "send_thread.h"

static void recv_notify_read(int sock, short event, void *arg);
static void server_sock_read(int sock, short event, void *arg);
static void client_sock_read(int sock, short event, void *arg);

static struct event_base *recv_event_base = NULL;
static struct event ev_notify;
static struct event ev_sock_server;
static int recv_fds[2] = {-1, -1};

int recv_notify_write()
{
	if (write(recv_fds[1], " ", 1) != 1)
	{
		logError("file: "__FILE__", line: %d, " \
			"call write failed, " \
			"errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno != 0 ? errno : EBADF;
	}

	return 0;
}

int kill_recv_thread()
{
	if (write(recv_fds[1], "\0", 1) != 1)
	{
		logError("file: "__FILE__", line: %d, " \
			"call write failed, " \
			"errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno != 0 ? errno : EBADF;
	}

	return 0;
}

void *recv_thread_entrance(void* arg)
{
	//int result;

	recv_event_base = event_base_new();
	if (recv_event_base == NULL)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"event_base_new fail.", __LINE__);
		g_continue_flag = false;
		return NULL;
	}

	event_set(&ev_sock_server, (int)arg, EV_READ | EV_PERSIST, \
		server_sock_read, &ev_sock_server);
	if (event_base_set(recv_event_base, &ev_sock_server) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"event_base_set fail.", __LINE__);
		g_continue_flag = false;
		return NULL;
	}
	if (event_add(&ev_sock_server, NULL) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"event_add fail.", __LINE__);
		g_continue_flag = false;
		return NULL;
	}

	if (pipe(recv_fds) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call pipe fail, " \
			"errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		g_continue_flag = false;
		return NULL;
	}

	if (set_nonblock(recv_fds[0]) != 0 || \
	    set_nonblock(recv_fds[1]) != 0)
	{
		g_continue_flag = false;
		return NULL;
	}

	event_set(&ev_notify, recv_fds[0], EV_READ | EV_PERSIST, \
		recv_notify_read, NULL);
	if (event_base_set(recv_event_base, &ev_notify) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"event_base_set fail.", __LINE__);
		g_continue_flag = false;
		return NULL;
	}
	if (event_add(&ev_notify, NULL) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"event_add fail.", __LINE__);
		g_continue_flag = false;
		return NULL;
	}

	while (g_continue_flag)
	{
		event_base_loop(recv_event_base, 0);
	}

	event_base_free(recv_event_base);

	close(recv_fds[0]);
	close(recv_fds[1]);

	logDebug("file: "__FILE__", line: %d, " \
		"recv thead exit.", __LINE__);

	return NULL;
}

static void server_sock_read(int sock, short event, void *arg)
{
	int incomesock;
	//int result;
	struct sockaddr_in inaddr;
	unsigned int sockaddr_len;
	struct task_info *pTask;
	char szClientIp[IP_ADDRESS_SIZE];
	in_addr_t client_addr;

	sockaddr_len = sizeof(inaddr);
	incomesock = accept(sock, (struct sockaddr*)&inaddr, &sockaddr_len);

	if (incomesock < 0) //error
	{
		logError("file: "__FILE__", line: %d, " \
			"accept failed, " \
			"errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return;
	}
	
	client_addr = getPeerIpaddr(incomesock, \
				szClientIp, IP_ADDRESS_SIZE);
	if (g_allow_ip_count >= 0)
	{
		if (bsearch(&client_addr, g_allow_ip_addrs, g_allow_ip_count, \
			sizeof(in_addr_t), cmp_by_ip_addr_t) == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"ip addr %s is not allowed to access", \
				__LINE__, szClientIp);

			close(incomesock);
			return;
		}
	}

	if (tcpsetnonblockopt(incomesock, g_network_timeout) != 0)
	{
		close(incomesock);
		return;
	}

	pTask = free_queue_pop();
	if (pTask == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc task buff failed", \
			__LINE__);
		close(incomesock);
		return;
	}

	strcpy(pTask->client_ip, szClientIp);
	event_set(&pTask->ev, incomesock, EV_READ, client_sock_read, pTask);
	if (event_base_set(recv_event_base, &pTask->ev) != 0)
	{
		free_queue_push(pTask);
		close(incomesock);

		logError("file: "__FILE__", line: %d, " \
			"event_base_set fail.", __LINE__);
		return;
	}
	if (event_add(&pTask->ev, &g_network_tv) != 0)
	{
		free_queue_push(pTask);
		close(incomesock);

		logError("file: "__FILE__", line: %d, " \
			"event_add fail.", __LINE__);
		return;
	}

	g_conn_count++;
}

static void client_sock_read(int sock, short event, void *arg)
{
	int bytes;
	int recv_bytes;
	struct task_info *pTask;

	pTask = (struct task_info *)arg;

	if (event == EV_TIMEOUT)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, recv timeout", \
			__LINE__, pTask->client_ip);

		close(pTask->ev.ev_fd);
		free_queue_push(pTask);
		g_recv_count++;
		return;
	}

	if (pTask->length == 0) //recv header
	{
		recv_bytes = sizeof(ProtoHeader);
	}
	else
	{
		recv_bytes = pTask->length - pTask->offset;
	}

	if (pTask->offset + recv_bytes > pTask->size)
	{
		char *pTemp;

		pTemp = pTask->data;
		pTask->size += recv_bytes;
		pTask->data = realloc(pTask->data, pTask->size);
		if (pTask->data == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"malloc failed, " \
				"errno: %d, error info: %s", \
				__LINE__, errno, strerror(errno));

			pTask->data = pTemp;  //restore old data

			close(pTask->ev.ev_fd);
			free_queue_push(pTask);
			g_recv_count++;
			return;
		}
	}

	bytes = recv(sock, pTask->data + pTask->offset, recv_bytes, 0);
	g_recv_count++;
	if (bytes < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, recv failed, " \
			"errno: %d, error info: %s", \
			__LINE__, pTask->client_ip, errno, strerror(errno));

		close(pTask->ev.ev_fd);
		free_queue_push(pTask);
		return;
	}
	else if (bytes == 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, recv failed, " \
			"connection disconnected.", \
			__LINE__, pTask->client_ip);

		close(pTask->ev.ev_fd);
		free_queue_push(pTask);
		return;
	}

	if (pTask->length == 0) //header
	{
		pTask->length = buff2int(((ProtoHeader *)pTask->data)->pkg_len);
		if (pTask->length < 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, pkg length: %d < 0", \
				__LINE__, pTask->client_ip, pTask->length);

			close(pTask->ev.ev_fd);
			free_queue_push(pTask);
			return;
		}

		pTask->length += sizeof(ProtoHeader);
		if (pTask->length > g_max_pkg_size)
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, pkg length: %d > max pkg " \
				"size: %d", __LINE__, pTask->client_ip, \
				pTask->length, g_max_pkg_size);

			close(pTask->ev.ev_fd);
			free_queue_push(pTask);
			return;
		}

		printf("pkg cmd: %d\n", ((ProtoHeader *)pTask->data)->cmd);
		printf("pkg length: %d\n", pTask->length);
	}

	pTask->offset += bytes;
	printf("pkg length: %d, pkg offset: %d\n", pTask->length, pTask->offset);
	if (pTask->offset >= pTask->length) //recv done
	{
		printf("recv done\n");
		//event_del(&pTask->ev);
		work_queue_push(pTask);
	}
	else if (event_add(&pTask->ev, &g_network_tv) != 0)
	{
		close(pTask->ev.ev_fd);
		free_queue_push(pTask);

		logError("file: "__FILE__", line: %d, " \
			"event_add fail.", __LINE__);
		return;
	}

	return;
}

static void recv_notify_read(int sock, short event, void *arg)
{
	struct task_info *pTask;
	char buff[1];

	if (read(recv_fds[0], buff, 1) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call read failed, " \
			"errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
	}

	if (*buff == '\0')
	{
		event_del(&ev_sock_server);
		event_del(&ev_notify);
	}

	while ((pTask = recv_queue_pop()) != NULL)
	{
		event_set(&pTask->ev, pTask->ev.ev_fd, EV_READ, \
			client_sock_read, pTask);
		if (event_base_set(recv_event_base, &pTask->ev) != 0)
		{
			close(pTask->ev.ev_fd);
			free_queue_push(pTask);

			logError("file: "__FILE__", line: %d, " \
				"event_base_set fail.", __LINE__);
			continue;
		}
		if (event_add(&pTask->ev, &g_network_tv) != 0)
		{
			close(pTask->ev.ev_fd);
			free_queue_push(pTask);

			logError("file: "__FILE__", line: %d, " \
				"event_add fail.", __LINE__);
			continue;
		}
	}
}

