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
#include "fdfs_global.h"
#include "fdht_global.h"
#include "ini_file_reader.h"
#include "sockopt.h"
#include "task_queue.h"
#include "recv_thread.h"
#include "send_thread.h"

bool bReloadFlag = false;
static pthread_attr_t thread_attr;
static pthread_cond_t thread_cond;
static pthread_condattr_t thread_condattr;
static pthread_mutex_t thread_mutex;

static void sigQuitHandler(int sig);
static void sigHupHandler(int sig);
static void sigUsrHandler(int sig);

static int create_worker(int server_sock);
static int init_pthread_cond();
static void wait_for_threads_exit();

static int g_done_count = 0;

int main(int argc, char *argv[])
{
	//char *conf_filename;
	char bind_addr[FDFS_IPADDR_SIZE];
	
	int result;
	int sock;
	struct sigaction act;

	printf("sizeof(task_info)=%d\n", sizeof(struct task_info));
	memset(bind_addr, 0, sizeof(bind_addr));
	/*
	if (argc < 2)
	{
		printf("Usage: %s <config_file>\n", argv[0]);
		return 1;
	}

	conf_filename = argv[1];
	if ((result=storage_load_from_conf_file(conf_filename, \
			bind_addr, sizeof(bind_addr))) != 0)
	{
		return result;
	}
	*/

	if ((result=init_pthread_lock(&g_storage_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_lock fail, program exit!", __LINE__);
		return result;
	}

	if ((result=init_pthread_lock(&thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_lock fail, program exit!", __LINE__);
		return result;
	}

	g_log_level = LOG_DEBUG;
	g_server_port = 12345;
	sock = socketServer(bind_addr, g_server_port, &result);
	if (sock < 0)
	{
		return result;
	}

	if ((result=tcpsetnonblockopt(sock, g_network_timeout)) != 0)
	{
		return result;
	}

	daemon_init(false);
	umask(0);
	
	memset(&act, 0, sizeof(act));
	sigemptyset(&act.sa_mask);

	act.sa_handler = sigUsrHandler;
	if(sigaction(SIGUSR1, &act, NULL) < 0 || \
		sigaction(SIGUSR2, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}

	act.sa_handler = sigHupHandler;
	if(sigaction(SIGHUP, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}
	
	act.sa_handler = SIG_IGN;
	if(sigaction(SIGPIPE, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}

	act.sa_handler = sigQuitHandler;
	if(sigaction(SIGINT, &act, NULL) < 0 || \
		sigaction(SIGTERM, &act, NULL) < 0 || \
		sigaction(SIGABRT, &act, NULL) < 0 || \
		sigaction(SIGSEGV, &act, NULL) < 0 || \
		sigaction(SIGQUIT, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		return errno;
	}

	if ((result=init_pthread_attr(&thread_attr)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_attr fail, program exit!", __LINE__);
		return result;
	}

	if ((result=init_pthread_cond()) != 0)
	{
		return result;
	}

	if ((result=task_queue_init()) != 0)
	{
		return result;
	}

	printf("queue count1: %d\n", free_queue_count() + recv_queue_count()); 
	create_worker(sock);

	printf("queue count2: %d\n", free_queue_count() + recv_queue_count()); 
	wait_for_threads_exit();

	pthread_attr_destroy(&thread_attr);
	pthread_mutex_destroy(&g_storage_thread_lock);
	pthread_mutex_destroy(&thread_mutex);

	pthread_condattr_destroy(&thread_condattr);
	pthread_cond_destroy(&thread_cond);

	printf("queue count3: %d\n", free_queue_count() + recv_queue_count()); 
	task_queue_destroy();
	close(sock);

	logInfo("exit nomally.\n");
	
	return 0;
}

static void wait_for_threads_exit()
{
	int i;
	int result;

	for (i=0; i<g_thread_count; i++)
	{
		if ((result=pthread_cond_signal(&thread_cond)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"pthread_cond_signal failed, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
		}
	}

	while (g_thread_count != 0)
	{
		if ((result=pthread_cond_signal(&thread_cond)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"pthread_cond_signal failed, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
		}

		sleep(1);
	}
}

static void sigQuitHandler(int sig)
{
	int result;
	if (g_continue_flag)
	{
		g_continue_flag = false;
		kill_recv_thread();
		kill_send_thread();
		logCrit("file: "__FILE__", line: %d, " \
			"catch signal %d, program exiting...", \
			__LINE__, sig);
		printf("g_conn_count: %d, g_recv_count: %d, g_send_count=%d, g_done_count=%d\n", 
			g_conn_count, g_recv_count, g_send_count, g_done_count);
		fflush(stdout);
		printf("free queue count: %d, task queue count: %d\n", free_queue_count(), recv_queue_count()); 
		fflush(stdout);
	}

	if ((result=pthread_cond_signal(&thread_cond)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"pthread_cond_signal failed, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

}

static void sigHupHandler(int sig)
{
	bReloadFlag = true;
}

static void sigUsrHandler(int sig)
{
	/*
	logInfo("current thread count=%d, " \
		"mo count=%d, success count=%d", g_thread_count, \
		nMoCount, nSuccMoCount);
	*/
}


/*
void* fdht_thread_entrance(void* arg)
{
	struct task_info *pTask;
	int result;

	pTask = NULL;
	while (g_continue_flag)
	{
		printf("####before lock....\n");
		if ((result=pthread_mutex_lock(&thread_mutex)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_lock fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
			sleep(1);
			continue;
		}

		printf("####before fetch....\n");
		while ((pTask = recv_queue_pop()) == NULL && g_continue_flag)
		{
			if ((result=pthread_cond_wait(&thread_cond, \
					&thread_mutex)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"pthread_cond_wait failed, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(result));
				break;
			}
			//printf("waked.!!!!!!!!!!!!!!, task=%08X\n", (int)pTask);

			//printf("before recv_queue_pop....\n");
			//printf("after recv_queue_pop. pTask=%08X\n", (int)pTask);
			//usleep(100);
			//break;
			pTask = recv_queue_pop();
			if (pTask != NULL)
			{
				break;
			}
		}
		//printf("####after fetch, task=%08X\n", (int)pTask);

		if ((result=pthread_mutex_unlock(&thread_mutex)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_unlock fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
		}

		if ((result=pthread_mutex_lock(&thread_mutex)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_lock fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
			sleep(1);
			continue;
		}

		pTask = recv_queue_pop();

		if ((result=pthread_mutex_unlock(&thread_mutex)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_unlock fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
		}

	//printf("queue count: %d\n", free_queue_count() + recv_queue_count()); 
	fflush(stdout);

		if (pTask == NULL)
		{
			continue;
		}
	//printf("after unlock thread task=%08X\n", (int)pTask);
	fflush(stdout);

		if (pTask->status < TASK_STATUS_SEND_BASE) //recv -> send
		{
			int fd;
			if (pTask->data == NULL)
			{
				g_continue_flag = 0;
				printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
				fflush(stdout);
				close(pTask->ev.ev_fd);
				free_queue_push(pTask);
				break;
			}

			pTask->length = sprintf(pTask->data, "HTTP/1.1 200 OK\r\n" \
							"Content-Type: text/html; charset=utf-8\r\n" \
							"Content-Length: 0\r\n" \
							"Connection: close\r\n" \
							"\r\n");
			pTask->offset = 0;

			fd = pTask->ev.ev_fd;
			//printf("pTask=%d, pTask->ev.ev_fd1=%d\n", (int)pTask, fd);

			//printf("event del: %d\n", event_del(&pTask->ev));
			//memset(&pTask->ev, 0, sizeof(pTask->ev));
			//event_set(&pTask->ev, fd, EV_WRITE, client_sock_write, pTask);
			//event_add(&pTask->ev, &tv);

			if ((result=pthread_mutex_lock(&g_storage_thread_lock)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"call pthread_mutex_lock fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(result));
			}

			event_set(&pTask->ev, fd, EV_WRITE, client_sock_write, pTask);
			event_add(&pTask->ev, &tv);

			if ((result=pthread_mutex_unlock(&g_storage_thread_lock)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"call pthread_mutex_unlock fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(result));
			}
			//printf("pTask->ev.ev_fd2=%d\n", fd);

			//close(pTask->ev.ev_fd);
			close(fd);
			free_queue_push(pTask);
			g_done_count++;
		}
		else //finish
		{
			//printf("status=%d, fd=%d closed!\n\n", pTask->status, pTask->ev.ev_fd);
			close(pTask->ev.ev_fd);
			free_queue_push(pTask);
			g_done_count++;
		}

		//printf("status=%d, offset=%d\n", pTask->status, pTask->offset);
		//printf("%.*s\n", pTask->offset, (char *)pTask->data);

		//break;
	}

	printf("thead exit.\n");
	fflush(stdout);

	if ((result=pthread_mutex_lock(&thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}
	g_thread_count--;
	if ((result=pthread_mutex_unlock(&thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	return NULL;
}
*/

static int create_worker(int server_sock)
{
	int i;
	int result;
	pthread_t recv_tid;
	pthread_t send_tid;
	pthread_t tid;

	g_thread_count = 0;
	result = 0;

	if ((result=pthread_create(&recv_tid, NULL, \
		recv_thread_entrance, (void *)server_sock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"create recv thread failed, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	if ((result=pthread_create(&send_tid, NULL, \
		send_thread_entrance, NULL)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"create send thread failed, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	/*
	for (i=0; i<g_max_threads; i++)
	{
		if ((result=pthread_create(&tid, &thread_attr, \
			fdht_thread_entrance, NULL)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"create thread failed, startup threads: %d, " \
				"errno: %d, error info: %s", \
				__LINE__, g_thread_count, \
				result, strerror(result));
			break;
		}
		else
		{
			if ((result=pthread_mutex_lock(&thread_mutex)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"call pthread_mutex_lock fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(result));
			}
			g_thread_count++;
			if ((result=pthread_mutex_unlock(&thread_mutex)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"call pthread_mutex_lock fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(result));
			}
		}
	}
	*/

	if ((result=pthread_join(recv_tid, NULL)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"pthread_join fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	if ((result=pthread_join(send_tid, NULL)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"pthread_join fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	return result;
}

static int init_pthread_cond()
{
	int result;
	if ((result=pthread_condattr_init(&thread_condattr)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"pthread_condattr_init failed, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	if ((result=pthread_cond_init(&thread_cond, &thread_condattr)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"pthread_cond_init failed, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	return 0;
}

