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
#include "accept_thread.h"
#include "recv_thread.h"

void *accept_thread_entrance(void* arg)
{
	int sock;
	int incomesock;
	struct sockaddr_in inaddr;
	unsigned int sockaddr_len;
	struct task_info *pTask;
	char szClientIp[IP_ADDRESS_SIZE];
	in_addr_t client_addr;

	sock = (int)arg;
	while (g_continue_flag)
	{
		sockaddr_len = sizeof(inaddr);
		incomesock = accept(sock, (struct sockaddr*)&inaddr, \
				&sockaddr_len);
		if (incomesock < 0) //error
		{
			logError("file: "__FILE__", line: %d, " \
				"accept failed, " \
				"errno: %d, error info: %s", \
				__LINE__, errno, strerror(errno));
			continue;
		}

		client_addr = getPeerIpaddr(incomesock, \
				szClientIp, IP_ADDRESS_SIZE);
		if (g_allow_ip_count >= 0)
		{
			if (bsearch(&client_addr, g_allow_ip_addrs, \
				g_allow_ip_count, sizeof(in_addr_t), \
				cmp_by_ip_addr_t) == NULL)
			{
				logError("file: "__FILE__", line: %d, " \
					"ip addr %s is not allowed to access", \
					__LINE__, szClientIp);

				close(incomesock);
				continue;
			}
		}

		if (tcpsetnonblockopt(incomesock, g_network_timeout) != 0)
		{
			close(incomesock);
			continue;
		}

		pTask = free_queue_pop();
		if (pTask == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
					"malloc task buff failed", \
					__LINE__);
			close(incomesock);
			continue;
		}

		strcpy(pTask->client_ip, szClientIp);
		pTask->ev.ev_fd = incomesock;
		recv_queue_push(pTask);
	}

	logDebug("file: "__FILE__", line: %d, " \
		"accept thread exit.", __LINE__);

	return NULL;
}

