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
#include "send_thread.h"
#include "work_thread.h"
#include "service.h"
#include "fdht_sync.h"

bool bReloadFlag = false;

static void sigQuitHandler(int sig);
static void sigHupHandler(int sig);
static void sigUsrHandler(int sig);

static int create_sock_io_threads(int server_sock);

static int g_done_count = 0;

int main(int argc, char *argv[])
{
	char *conf_filename;
	char bind_addr[IP_ADDRESS_SIZE];
	
	int result;
	int sock;
	struct sigaction act;

	memset(bind_addr, 0, sizeof(bind_addr));
	if (argc < 2)
	{
		printf("Usage: %s <config_file>\n", argv[0]);
		return 1;
	}

	conf_filename = argv[1];
	if ((result=fdht_service_init(conf_filename, \
			bind_addr, sizeof(bind_addr))) != 0)
	{
		return result;
	}

	if ((result=init_pthread_lock(&g_storage_thread_lock)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_lock fail, program exit!", __LINE__);
		fdht_service_destroy();
		return result;
	}

	g_log_level = LOG_DEBUG;
	sock = socketServer(bind_addr, g_server_port, &result);
	if (sock < 0)
	{
		fdht_service_destroy();
		return result;
	}

	if ((result=tcpsetnonblockopt(sock, g_network_timeout)) != 0)
	{
		fdht_service_destroy();
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
		fdht_service_destroy();
		return errno;
	}

	act.sa_handler = sigHupHandler;
	if(sigaction(SIGHUP, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		fdht_service_destroy();
		return errno;
	}
	
	act.sa_handler = SIG_IGN;
	if(sigaction(SIGPIPE, &act, NULL) < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call sigaction fail, errno: %d, error info: %s", \
			__LINE__, errno, strerror(errno));
		fdht_service_destroy();
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
		fdht_service_destroy();
		return errno;
	}

	if ((result=fdht_sync_init()) != 0)
	{
		fdht_service_destroy();
		return result;
	}

	if ((result=task_queue_init()) != 0)
	{
		fdht_service_destroy();
		return result;
	}

	printf("queue count1: %d\n", free_queue_count() + recv_queue_count()); 
	if ((result=work_thread_init()) != 0)
	{
		fdht_service_destroy();
		return result;
	}

	if ((result=create_sock_io_threads(sock)) != 0)
	{
		work_thread_destroy();
		fdht_service_destroy();
		return result;
	}

	printf("queue count2: %d\n", free_queue_count() + recv_queue_count()); 

	pthread_mutex_destroy(&g_storage_thread_lock);

	work_thread_destroy();

	task_queue_destroy();
	printf("queue count3: %d\n", free_queue_count() + recv_queue_count()); 
	close(sock);

	fdht_sync_destroy();	
	fdht_service_destroy();

	logInfo("exit nomally.\n");
	
	return 0;
}

static void sigQuitHandler(int sig)
{
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

static int create_sock_io_threads(int server_sock)
{
	int result;
	pthread_t recv_tid;
	pthread_t send_tid;

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

