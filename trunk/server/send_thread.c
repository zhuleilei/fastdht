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
#include "fdht_proto.h"
#include "recv_thread.h"

static void send_notify_read(int sock, short event, void *arg);
static void client_sock_write(int sock, short event, void *arg);

static int send_fds[2] = {-1, -1};
static struct event ev_notify;

int send_notify_write()
{
	if (write(send_fds[1], " ", 1) != 1)
	{
		logError("file: "__FILE__", line: %d, " \
			"call write failed, " \
			"errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));

		return errno != 0 ? errno : EBADF;
	}

	return 0;
}

int send_thread_init()
{
	int result;

	if (pipe(send_fds) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call pipe fail, " \
			"errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno != 0 ? errno : EMFILE;
	}

	if ((result=set_nonblock(send_fds[0])) != 0)
	{
		return result;
	}

	event_set(&ev_notify, send_fds[0], EV_READ | EV_PERSIST, \
		send_notify_read, NULL);
	if ((result=event_base_set(g_event_base, &ev_notify)) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"event_base_set fail.", __LINE__);
		return result;
	}
	if ((result=event_add(&ev_notify, NULL)) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"event_add fail.", __LINE__);
		return result;
	}

	return 0;
}

int send_process_init()
{
	return 0;
}

int send_add_event(struct task_info *pTask)
{
	//int result;

	pTask->offset = 0;

	client_sock_write(pTask->ev_write.ev_fd, EV_WRITE, pTask);

	/*
	event_set(&pTask->ev_write, pTask->ev_write.ev_fd, EV_WRITE, \
			client_sock_write, pTask);
	if ((result=event_base_set(g_event_base, &pTask->ev_write)) != 0)
	{
		close(pTask->ev_write.ev_fd);
		free_queue_push(pTask);

		logError("file: "__FILE__", line: %d, " \
			"event_base_set fail.", __LINE__);
		return result;
	}
	if ((result=event_add(&pTask->ev_write, &g_network_tv)) != 0)
	{
		close(pTask->ev_write.ev_fd);
		free_queue_push(pTask);

		logError("file: "__FILE__", line: %d, " \
				"event_add fail.", __LINE__);
		return result;
	}
	*/

	return 0;
}

int send_set_event(struct task_info *pTask, int sock)
{
	int result;

	event_set(&pTask->ev_write, sock, EV_WRITE, \
			client_sock_write, pTask);
	if ((result=event_base_set(g_event_base, &pTask->ev_write)) != 0)
	{
		close(pTask->ev_write.ev_fd);
		free_queue_push(pTask);

		logError("file: "__FILE__", line: %d, " \
			"event_base_set fail.", __LINE__);
		return result;
	}

	return 0;
}

static void client_sock_write(int sock, short event, void *arg)
{
	int bytes;
	struct task_info *pTask;

	pTask = (struct task_info *)arg;
	if (event == EV_TIMEOUT)
	{
		logError("file: "__FILE__", line: %d, " \
			"send timeout", __LINE__);

		close(pTask->ev_write.ev_fd);
		free_queue_push(pTask);

		return;
	}

	while (1)
	{
	bytes = send(sock, pTask->data + pTask->offset, \
			pTask->length - pTask->offset,  0);
	//printf("%08X sended %d bytes\n", (int)pTask, bytes);
	if (bytes < 0)
	{
		if (errno == EAGAIN || errno == EWOULDBLOCK)
		{
			if (event_add(&pTask->ev_write, &g_network_tv) != 0)
			{
				close(pTask->ev_write.ev_fd);
				free_queue_push(pTask);

				logError("file: "__FILE__", line: %d, " \
					"event_add fail.", __LINE__);
			}
		}
		else
		{
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, recv failed, " \
				"errno: %d, error info: %s", \
				__LINE__, pTask->client_ip, \
				errno, strerror(errno));

			close(pTask->ev_write.ev_fd);
			free_queue_push(pTask);
		}

		return;
	}
	else if (bytes == 0)
	{
		logWarning("file: "__FILE__", line: %d, " \
			"send failed, connection disconnected.", \
			__LINE__);

		close(pTask->ev_write.ev_fd);
		free_queue_push(pTask);
		return;
	}

	pTask->offset += bytes;
	if (pTask->offset >= pTask->length)
	{
		if (((FDHTProtoHeader *)pTask->data)->keep_alive)
		{
			recv_add_event(pTask);
		}
		else
		{
			close(pTask->ev_write.ev_fd);
			free_queue_push(pTask);
		}

		return;
	}
	}

	/*
	if (event_add(&pTask->ev_write, &g_network_tv) != 0)
	{
		close(pTask->ev_write.ev_fd);
		free_queue_push(pTask);

		logError("file: "__FILE__", line: %d, " \
			"event_add fail.", __LINE__);
	}
	*/
}

static void send_notify_read(int sock, short event, void *arg)
{
	struct task_info *pTask;
	char buff[1024];
	int bytes;
	int total_bytes;

	total_bytes = 0;
	while (1)
	{
		if ((bytes=read(send_fds[0], buff, sizeof(buff))) < 0)
		{
			if (!(errno == EAGAIN || errno == EWOULDBLOCK))
			{
				logError("file: "__FILE__", line: %d, " \
					"call read failed, " \
					"errno: %d, error info: %s", \
					__LINE__, errno, strerror(errno));
			}

			break;
		}
		else if (bytes == 0)
		{
			break;
		}

		total_bytes += bytes;
		if (bytes < sizeof(buff))
		{
			break;
		}
	}

	if (total_bytes == 0)
	{
		logInfo("send_thread total_bytes==0!");
		return;
	}

	while ((pTask = send_queue_pop()) != NULL)
	{
		event_set(&pTask->ev_write, pTask->ev_write.ev_fd, EV_WRITE, \
			client_sock_write, pTask);
		if (event_base_set(g_event_base, &pTask->ev_write) != 0)
		{
			close(pTask->ev_write.ev_fd);
			free_queue_push(pTask);

			logError("file: "__FILE__", line: %d, " \
				"event_base_set fail.", __LINE__);
			continue;
		}
		if (event_add(&pTask->ev_write, &g_network_tv) != 0)
		{
			close(pTask->ev_write.ev_fd);
			free_queue_push(pTask);

			logError("file: "__FILE__", line: %d, " \
				"event_add fail.", __LINE__);
			continue;
		}
	}
}

