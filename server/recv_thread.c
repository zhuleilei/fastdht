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
	
	//printf("incomesock=%d\n", incomesock);
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
#define RECV_BYTES_ONCE  (2 * 1024)
	int bytes;
	//int result;
	struct task_info *pTask;

	pTask = (struct task_info *)arg;

	if (event == EV_TIMEOUT)
	{
		logError("file: "__FILE__", line: %d, " \
			"recv timeout", __LINE__);

		close(pTask->ev.ev_fd);
		free_queue_push(pTask);
		g_recv_count++;
		return;
	}

	if (pTask->offset + RECV_BYTES_ONCE > pTask->size)
	{
		char *pTemp;

		printf("realloc!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1!!!!!!!!!!!!!!!!!!!!\n");
		pTemp = pTask->data;
		pTask->size += RECV_BYTES_ONCE;
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

	bytes = recv(sock, pTask->data + pTask->offset, \
			RECV_BYTES_ONCE,  0);
	if (bytes < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"recv failed, " \
			"errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));

		close(pTask->ev.ev_fd);
		free_queue_push(pTask);
		g_recv_count++;
		return;
	}
	else if (bytes == 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"recv failed, connection disconnected.", \
			__LINE__);

		close(pTask->ev.ev_fd);
		free_queue_push(pTask);
		g_recv_count++;
		return;
	}

	pTask->offset += bytes;


	//for test start.........................
	pTask->length = sprintf(pTask->data, "HTTP/1.1 200 OK\r\n" \
					"Content-Type: text/html; charset=utf-8\r\n" \
					"Content-Length: 0\r\n" \
					"Connection: close\r\n" \
					"\r\n");
	pTask->offset = 0;

	send_queue_push(pTask);

	//for test end.........................

	g_recv_count++;

	/*
	if (event_add(&pTask->ev, &g_network_tv) != 0)
	{
		close(pTask->ev.ev_fd);
		free_queue_push(pTask);

		logError("file: "__FILE__", line: %d, " \
			"event_add fail.", __LINE__);
		return;
	}
	*/

	return;

	//printf("recv total length: %d\n", pTask->offset);
	/*
	if ((result=pthread_cond_signal(&thread_cond)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"pthread_cond_signal failed, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}
	*/

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
