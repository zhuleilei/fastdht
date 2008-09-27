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
#include "fdht_types.h"
#include "fdht_proto.h"
#include "global.h"
#include "ini_file_reader.h"
#include "sockopt.h"
#include "task_queue.h"
#include "recv_thread.h"
#include "send_thread.h"
#include "service.h"
#include "db_op.h"

static pthread_mutex_t work_thread_mutex;
static pthread_cond_t work_thread_cond;
static int init_pthread_cond();

static int g_done_count = 0;

static void *work_thread_entrance(void* arg);
static void wait_for_work_threads_exit();
static int deal_task(struct task_info *pTask);

static int deal_cmd_get(struct task_info *pTask);
static int deal_cmd_set(struct task_info *pTask);

int work_thread_init()
{
	int i;
	int result;
	pthread_t tid;
	pthread_attr_t thread_attr;

	if ((result=init_pthread_lock(&work_thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"init_pthread_lock fail, program exit!", __LINE__);
		return result;
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

	g_thread_count = 0;
	for (i=0; i<g_max_threads; i++)
	{
		if ((result=pthread_create(&tid, &thread_attr, \
			work_thread_entrance, NULL)) != 0)
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
			if ((result=pthread_mutex_lock(&work_thread_mutex)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"call pthread_mutex_lock fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(result));
			}
			g_thread_count++;
			if ((result=pthread_mutex_unlock(&work_thread_mutex)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"call pthread_mutex_lock fail, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(result));
			}
		}
	}

	pthread_attr_destroy(&thread_attr);

	return 0;
}

void work_thread_destroy()
{
	wait_for_work_threads_exit();

	pthread_mutex_destroy(&work_thread_mutex);
	pthread_cond_destroy(&work_thread_cond);
}

int work_notify_task()
{
	int result;
	if ((result=pthread_cond_signal(&work_thread_cond)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"pthread_cond_signal failed, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	return result;
}

static void wait_for_work_threads_exit()
{
	int i;
	int result;

	for (i=0; i<g_thread_count; i++)
	{
		if ((result=pthread_cond_signal(&work_thread_cond)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"pthread_cond_signal failed, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
		}
	}

	while (g_thread_count != 0)
	{
		if ((result=pthread_cond_signal(&work_thread_cond)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"pthread_cond_signal failed, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
		}

		sleep(1);
	}
}

static void *work_thread_entrance(void* arg)
{
	struct task_info *pTask;
	int result;

	while (g_continue_flag)
	{
		if ((result=pthread_mutex_lock(&work_thread_mutex)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_lock fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
			sleep(1);
			continue;
		}

		while ((pTask = work_queue_pop()) == NULL && g_continue_flag)
		{
			if ((result=pthread_cond_wait(&work_thread_cond, \
					&work_thread_mutex)) != 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"pthread_cond_wait failed, " \
					"errno: %d, error info: %s", \
					__LINE__, result, strerror(result));
				break;
			}
		}

		if ((result=pthread_mutex_unlock(&work_thread_mutex)) != 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"call pthread_mutex_unlock fail, " \
				"errno: %d, error info: %s", \
				__LINE__, result, strerror(result));
		}

		if (pTask == NULL)
		{
			continue;
		}

		deal_task(pTask);

		g_done_count++;
	}

	printf("thead exit.\n");
	fflush(stdout);

	if ((result=pthread_mutex_lock(&work_thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_lock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}
	g_thread_count--;
	if ((result=pthread_mutex_unlock(&work_thread_mutex)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"call pthread_mutex_unlock fail, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
	}

	return NULL;
}

static int init_pthread_cond()
{
	int result;
	pthread_condattr_t thread_condattr;
	if ((result=pthread_condattr_init(&thread_condattr)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"pthread_condattr_init failed, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	if ((result=pthread_cond_init(&work_thread_cond, &thread_condattr)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"pthread_cond_init failed, " \
			"errno: %d, error info: %s", \
			__LINE__, result, strerror(result));
		return result;
	}

	pthread_condattr_destroy(&thread_condattr);
	return 0;
}

static int deal_task(struct task_info *pTask)
{
	ProtoHeader *pHeader;

	pHeader = (ProtoHeader *)pTask->data;
	switch(pHeader->cmd)
	{
		case FDHT_PROTO_CMD_GET:
			pHeader->status = deal_cmd_get(pTask);
		case FDHT_PROTO_CMD_SET:
			pHeader->status = deal_cmd_set(pTask);
			break;
		case FDHT_PROTO_CMD_INC:
			break;
		case FDHT_PROTO_CMD_DEL:
			break;
		case FDHT_PROTO_CMD_QUIT:
			close(pTask->ev.ev_fd);
			free_queue_push(pTask);
			return 0;
		default:
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, invalid cmd: 0x%02X", \
				__LINE__, pTask->client_ip, pHeader->cmd);
			pHeader->status = EINVAL;
			pTask->length = sizeof(ProtoHeader);
			break;
	}

	pHeader->cmd = FDHT_PROTO_CMD_RESP;
	int2buff(pTask->length - sizeof(ProtoHeader), pHeader->pkg_len);

	send_queue_push(pTask);

	return 0;
}

#define CHECK_GROUP_ID(pTask, group_id) \
	group_id = buff2int(((ProtoHeader *)pTask->data)->group_id); \
	if (group_id < 0 || group_id >= g_db_count) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid group_id: %d, " \
			"which < 0 or >= %d", \
			__LINE__, pTask->client_ip, group_id, g_db_count); \
		pTask->length = sizeof(ProtoHeader); \
		return  EINVAL; \
	} \
	if (g_db_list[group_id] == NULL) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid group_id: %d, " \
			"which does not belong to this server", \
			__LINE__, pTask->client_ip, group_id); \
		pTask->length = sizeof(ProtoHeader); \
		return  EINVAL; \
	} \


/**
* request body format:
*       key_len:  4 bytes big endian integer
*       key:      key name
* response body format:
*       value_len:  4 bytes big endian integer
*       value:      value buff
*/
static int deal_cmd_get(struct task_info *pTask)
{
	int nInBodyLen;
	int key_len;
	char *key;
	int group_id;
	char *pValue;
	int value_len;
	int result;

	CHECK_GROUP_ID(pTask, group_id)

	nInBodyLen = pTask->length - sizeof(ProtoHeader);
	if (nInBodyLen <= 4)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d <= 4", \
			__LINE__, pTask->client_ip, nInBodyLen);
		pTask->length = sizeof(ProtoHeader);
		return  EINVAL;
	}

	key_len = buff2int(pTask->data + sizeof(ProtoHeader));
	if (nInBodyLen != 4 + key_len)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, 4 + key_len);
		pTask->length = sizeof(ProtoHeader);
		return  EINVAL;
	}

	key = pTask->data + sizeof(ProtoHeader) + 4;
	pValue = NULL;
	if ((result=db_get(g_db_list[group_id], key, key_len, \
               	&pValue, &value_len)) != 0)
	{
		pTask->length = sizeof(ProtoHeader);
		return result;
	}

	pTask->length = sizeof(ProtoHeader) + 4 + value_len;
	if (pTask->length > pTask->size)
	{
		char *pTemp;
		pTemp = pTask->data;
		pTask->size = pTask->length;
		pTask->data = realloc(pTask->data, pTask->size);
		if (pTask->data == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"malloc failed, " \
				"errno: %d, error info: %s", \
				__LINE__, errno, strerror(errno));

			pTask->data = pTemp;  //restore old data
			pTask->length = sizeof(ProtoHeader);
			return ENOMEM;
		}
	}

	int2buff(value_len, pTask->data+sizeof(ProtoHeader));
	memcpy(pTask->data+sizeof(ProtoHeader)+4, pValue, value_len);
	free(pValue);

	return 0;
}

/**
* request body format:
*       key_len:  4 bytes big endian integer
*       key:      key name
*       value_len:  4 bytes big endian integer
*       value:      value buff
* response body format:
*      none
*/
static int deal_cmd_set(struct task_info *pTask)
{
	int nInBodyLen;
	int key_len;
	char *key;
	int group_id;
	char *pValue;
	int value_len;

	CHECK_GROUP_ID(pTask, group_id)

	nInBodyLen = pTask->length - sizeof(ProtoHeader);
	if (nInBodyLen <= 8)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d <= 8", \
			__LINE__, pTask->client_ip, nInBodyLen);
		pTask->length = sizeof(ProtoHeader);
		return  EINVAL;
	}

	key_len = buff2int(pTask->data + sizeof(ProtoHeader));
	if (key_len <= 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, key length: %d <= 0", \
			__LINE__, pTask->client_ip, key_len);
		pTask->length = sizeof(ProtoHeader);
		return  EINVAL;
	}
	if (nInBodyLen < 8 + key_len)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d < %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, 8 + key_len);
		pTask->length = sizeof(ProtoHeader);
		return  EINVAL;
	}
	key = pTask->data + sizeof(ProtoHeader) + 4;

	value_len = buff2int(pTask->data + sizeof(ProtoHeader) + 4 + key_len);
	if (value_len < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, value length: %d < 0", \
			__LINE__, pTask->client_ip, key_len);
		pTask->length = sizeof(ProtoHeader);
		return  EINVAL;
	}
	if (nInBodyLen != 8 + key_len + value_len)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, 8 + key_len + value_len);
		pTask->length = sizeof(ProtoHeader);
		return  EINVAL;
	}
	pValue = pTask->data + sizeof(ProtoHeader) + 8 + key_len;

	pTask->length = sizeof(ProtoHeader);
	return db_set(g_db_list[group_id], key, key_len, pValue, value_len);
}

