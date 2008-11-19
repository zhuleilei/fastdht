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
#include "recv_thread.h"

static void send_notify_read(int sock, short event, void *arg);
static void client_sock_write(int sock, short event, void *arg);

static struct event_base *send_event_base = NULL;
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

int kill_send_thread()
{
	if (send_fds[1] >= 0 && write(send_fds[1], "\0", 1) != 1)
	{
		logError("file: "__FILE__", line: %d, " \
			"call write failed, " \
			"errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno != 0 ? errno : EBADF;
	}

	return 0;
}

void *send_thread_entrance(void* arg)
{
	send_event_base = event_base_new();
	if (send_event_base == NULL)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"event_base_new fail.", __LINE__);
		g_continue_flag = false;
		return NULL;
	}

	if (pipe(send_fds) != 0)
	{
		logCrit("file: "__FILE__", line: %d, " \
			"call pipe fail, " \
			"errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		g_continue_flag = false;
		return NULL;
	}

	if (set_nonblock(send_fds[0]) != 0 || \
	    set_nonblock(send_fds[1]) != 0)
	{
		g_continue_flag = false;
		return NULL;
	}

	event_set(&ev_notify, send_fds[0], EV_READ | EV_PERSIST, \
		send_notify_read, NULL);
	if (event_base_set(send_event_base, &ev_notify) != 0)
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
		event_base_loop(send_event_base, 0);
	}

	event_base_free(send_event_base);

	close(send_fds[0]);
	close(send_fds[1]);

	logDebug("file: "__FILE__", line: %d, " \
		"send thead exit.", __LINE__);

	return NULL;
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

		close(pTask->ev.ev_fd);
		free_queue_push(pTask);

		g_send_count++;
		return;
	}

	bytes = send(sock, pTask->data + pTask->offset, \
			pTask->length - pTask->offset,  0);
	//printf("%08X sended %d bytes\n", (int)pTask, bytes);
	if (bytes < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"send failed, " \
			"errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));

		close(pTask->ev.ev_fd);
		free_queue_push(pTask);
		g_send_count++;
		return;
	}
	else if (bytes == 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"send failed, connection disconnected.", \
			__LINE__);

		close(pTask->ev.ev_fd);
		free_queue_push(pTask);
		g_send_count++;
		return;
	}

	pTask->offset += bytes;
	if (pTask->offset >= pTask->length)
	{
		recv_queue_push(pTask);  //persistent connection
		g_send_count++;
		return;
	}

	if (event_add(&pTask->ev, &g_network_tv) != 0)
	{
		close(pTask->ev.ev_fd);
		free_queue_push(pTask);

		logError("file: "__FILE__", line: %d, " \
			"event_add fail.", __LINE__);
	}
}

static void send_notify_read(int sock, short event, void *arg)
{
	struct task_info *pTask;
	char buff[1];

	if (read(send_fds[0], buff, 1) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call read failed, " \
			"errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
	}

	if (*buff == '\0')  //quit
	{
		event_del(&ev_notify);
	}

	while ((pTask = send_queue_pop()) != NULL)
	{
		event_set(&pTask->ev, pTask->ev.ev_fd, EV_WRITE, \
			client_sock_write, pTask);
		if (event_base_set(send_event_base, &pTask->ev) != 0)
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

