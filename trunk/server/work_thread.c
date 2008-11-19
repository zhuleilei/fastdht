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
#include "fdht_define.h"
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
#include "func.h"
#include "db_op.h"
#include "sync.h"

#define SYNC_REQ_WAIT_SECONDS	60 * 60

static pthread_mutex_t work_thread_mutex;
static pthread_cond_t work_thread_cond;
static int init_pthread_cond();
static time_t first_sync_req_time = 0;

static int g_done_count = 0;

static void *work_thread_entrance(void* arg);
static void wait_for_work_threads_exit();
static int deal_task(struct task_info *pTask);

static int deal_cmd_get(struct task_info *pTask);
static int deal_cmd_set(struct task_info *pTask, byte op_type);
static int deal_cmd_del(struct task_info *pTask, byte op_type);
static int deal_cmd_inc(struct task_info *pTask);
static int deal_cmd_sync_req(struct task_info *pTask);
static int deal_cmd_sync_done(struct task_info *pTask);

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

	//printf("thead exit.\n");
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
			break;
		case FDHT_PROTO_CMD_SET:
			pHeader->status = deal_cmd_set(pTask, \
					FDHT_OP_TYPE_SOURCE_SET);
			break;
		case FDHT_PROTO_CMD_SYNC_SET:
			pHeader->status = deal_cmd_set(pTask, \
					FDHT_OP_TYPE_REPLICA_SET);
			break;
		case FDHT_PROTO_CMD_INC:
			pHeader->status = deal_cmd_inc(pTask);
			break;
		case FDHT_PROTO_CMD_DEL:
			pHeader->status = deal_cmd_del(pTask, \
					FDHT_OP_TYPE_SOURCE_DEL);
			break;
		case FDHT_PROTO_CMD_SYNC_DEL:
			pHeader->status = deal_cmd_del(pTask, \
					FDHT_OP_TYPE_REPLICA_DEL);
			break;
		case FDHT_PROTO_CMD_HEART_BEAT:
			pTask->length = sizeof(ProtoHeader);
			pHeader->status = 0;
			break;
		case FDHT_PROTO_CMD_QUIT:
			close(pTask->ev.ev_fd);
			free_queue_push(pTask);
			return 0;
		case FDHT_PROTO_CMD_SYNC_REQ:
			pHeader->status = deal_cmd_sync_req(pTask);
			break;
		case FDHT_PROTO_CMD_SYNC_NOTIFY:
			pHeader->status = deal_cmd_sync_done(pTask);
			break;
		default:
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, invalid cmd: 0x%02X", \
				__LINE__, pTask->client_ip, pHeader->cmd);
			pHeader->status = EINVAL;
			pTask->length = sizeof(ProtoHeader);
			break;
	}

	//printf("cmd=%d, resp pkg_len=%d\n", pHeader->cmd, pTask->length - sizeof(ProtoHeader));
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

#define PACK_SYNC_REQ_BODY(pTask) \
	pTask->length = sizeof(ProtoHeader) + 1 + IP_ADDRESS_SIZE + 8; \
	*(pTask->data + sizeof(ProtoHeader)) = g_sync_old_done; \
	memcpy(pTask->data + sizeof(ProtoHeader) + 1, \
		g_sync_src_ip_addr, IP_ADDRESS_SIZE); \
	int2buff(g_sync_src_port, pTask->data + \
		sizeof(ProtoHeader) + 1 + IP_ADDRESS_SIZE); \
	int2buff(g_sync_until_timestamp, pTask->data + \
		sizeof(ProtoHeader) + 1 + IP_ADDRESS_SIZE + 4);

/**
* request body format:
*      server port : 4 bytes
*      sync_old_done: 1 byte
*      update count: 8 bytes
* response body format:
*      sync_old_done: 1 byte
*      sync_src_ip_addr: IP_ADDRESS_SIZE bytes
*      sync_src_port:  4 bytes
*      sync_until_timestamp: 4 bytes
*/
static int deal_cmd_sync_req(struct task_info *pTask)
{
	int result;
	int nInBodyLen;
	int64_t update_count;
	FDHTGroupServer targetServer;
	FDHTGroupServer *pFound;
	FDHTGroupServer *pServer;
	FDHTGroupServer *pEnd;
	FDHTGroupServer *pFirstServer;
	FDHTGroupServer *pMaxCountServer;
	bool src_sync_old_done;

	nInBodyLen = pTask->length - sizeof(ProtoHeader);
	if (nInBodyLen != 13)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != 13", \
			__LINE__, pTask->client_ip, nInBodyLen);
		pTask->length = sizeof(ProtoHeader);
		return EINVAL;
	}

	if (g_sync_old_done)
	{
		PACK_SYNC_REQ_BODY(pTask)
		return 0;
	}

	memset(&targetServer, 0, sizeof(FDHTGroupServer));
	strcpy(targetServer.ip_addr, pTask->client_ip);
	targetServer.port = buff2int(pTask->data + sizeof(ProtoHeader));
	src_sync_old_done = *(pTask->data + sizeof(ProtoHeader) + 4);
	update_count = buff2long(pTask->data + sizeof(ProtoHeader) + 5);

	pFound = (FDHTGroupServer *)bsearch(&targetServer, \
			g_group_servers, g_group_server_count, \
			sizeof(FDHTGroupServer),group_cmp_by_ip_and_port);
	if (pFound == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"server: %s:%d not in my group!", \
			__LINE__, pTask->client_ip, targetServer.port);
		pTask->length = sizeof(ProtoHeader);
		return ENOENT;
	}

	if (first_sync_req_time == 0)
	{
		first_sync_req_time = time(NULL);
	}

	pFound->sync_old_done = src_sync_old_done;
	pFound->sync_req_count++;
	pFound->update_count = update_count;

	pEnd = g_group_servers + g_group_server_count;
	pFirstServer = g_group_servers;
	while (pFirstServer < pEnd && is_local_host_ip(pFirstServer->ip_addr))
	{
		pFirstServer++;
	}

	if (pFirstServer == pEnd) //impossible
	{
		logError("file: "__FILE__", line: %d, " \
			"client: %s:%d, the ip addresses of all servers " \
			"are local ip addresses.", __LINE__, \
			pTask->client_ip, targetServer.port);
		pTask->length = sizeof(ProtoHeader);
		return ENOENT;
	}

	while (1)
	{
		if (pFirstServer->sync_req_count > 0 && pFirstServer->sync_old_done)
		{
			pServer = pFirstServer;
			break;
		}

		pServer = pFirstServer;
		while (pServer < pEnd)
		{
			if (is_local_host_ip(pServer->ip_addr))
			{
				pServer++;
				continue;
			}

			if (pServer->sync_req_count == 0)
			{
				break;
			}

			if (pServer->sync_old_done)
			{
				break;
			}

			pServer++;
		}

		if (pServer >= pEnd) //all is new server?
		{
			pMaxCountServer = pFirstServer;
			pServer = pFirstServer + 1;
			while (pServer < pEnd)
			{
				if (is_local_host_ip(pServer->ip_addr))
				{
					pServer++;
					continue;
				}

				if (pServer->update_count > pMaxCountServer->update_count)
				{
					pMaxCountServer = pServer;
				}

				pServer++;
			}

			pServer = pMaxCountServer;
			break;
		}

		if (time(NULL) - first_sync_req_time < SYNC_REQ_WAIT_SECONDS)
		{
			pTask->length = sizeof(ProtoHeader);
			return EAGAIN;
		}

		pServer = pFirstServer + 1;
		while (pServer < pEnd)
		{
			if (pServer->sync_req_count > 0 && \
			     pServer->sync_old_done && \
			     !is_local_host_ip(pServer->ip_addr))
			{
				break;
			}

			pServer++;
		}

		if (pServer >= pEnd)
		{
			pTask->length = sizeof(ProtoHeader);
			return ENOENT;
		}

		break;
	}

	if (!(strcmp(pTask->client_ip, pServer->ip_addr) == 0 && \
		targetServer.port == pServer->port))
	{
		pTask->length = sizeof(ProtoHeader);
		return EAGAIN;
	}

	if (pServer->update_count > 0)
	{
		strcpy(g_sync_src_ip_addr, pServer->ip_addr);
		g_sync_src_port = pServer->port;
		g_sync_until_timestamp = time(NULL);
	}
	else
	{
		g_sync_old_done = true;  //no old data to sync
	}

	if ((result=write_to_sync_ini_file()) != 0)
	{
		pTask->length = sizeof(ProtoHeader);
		return result;
	}

	PACK_SYNC_REQ_BODY(pTask)
	return 0;
}

static int deal_cmd_sync_done(struct task_info *pTask)
{
	int result;
	int nInBodyLen;
	int src_port;

	nInBodyLen = pTask->length - sizeof(ProtoHeader);
	if (nInBodyLen != 4)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != 4", \
			__LINE__, pTask->client_ip, nInBodyLen);
		pTask->length = sizeof(ProtoHeader);
		return EINVAL;
	}

	pTask->length = sizeof(ProtoHeader);
	src_port = buff2int(pTask->data + sizeof(ProtoHeader));
	if (!(strcmp(pTask->client_ip, g_sync_src_ip_addr) == 0 && \
		src_port == g_sync_src_port))
	{
		logError("file: "__FILE__", line: %d, " \
			"server: %s:%d not the sync src server!", \
			__LINE__, pTask->client_ip, src_port);
		return EINVAL;
	}

	if (g_sync_old_done)
	{
		return 0;
	}

	g_sync_old_done = true;
	g_sync_done_timestamp = time(NULL);
	if ((result=write_to_sync_ini_file()) != 0)
	{
		return result;
	}

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
static int deal_cmd_set(struct task_info *pTask, byte op_type)
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

	result = db_set(g_db_list[group_id], key, key_len, pValue, value_len);
	if (result == 0)
	{
		fdht_binlog_write(op_type, key, key_len, pValue, value_len);
	}

	return result;
}

/**
* request body format:
*       key_len:  4 bytes big endian integer
*       key:      key name
* response body format:
*      none
*/
static int deal_cmd_del(struct task_info *pTask, byte op_type)
{
	int nInBodyLen;
	int key_len;
	char *key;
	int group_id;
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

	pTask->length = sizeof(ProtoHeader);
	result = db_delete(g_db_list[group_id], key, key_len);
	if (result == 0)
	{
		fdht_binlog_write(op_type, key, key_len, NULL, 0);
	}

	return result;
}

/**
* request body format:
*       key_len:  4 bytes big endian integer
*       key:      key name
*       incr      4 bytes big endian integer
* response body format:
*      none
*/
static int deal_cmd_inc(struct task_info *pTask)
{
	int nInBodyLen;
	char *key;
	int key_len;
	int group_id;
	int inc;
	char value[32];
	int value_len;
	int result;

	CHECK_GROUP_ID(pTask, group_id)

	nInBodyLen = pTask->length - sizeof(ProtoHeader);
	if (nInBodyLen <= 8)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d <= 4", \
			__LINE__, pTask->client_ip, nInBodyLen);
		pTask->length = sizeof(ProtoHeader);
		return  EINVAL;
	}

	key_len = buff2int(pTask->data + sizeof(ProtoHeader));
	if (nInBodyLen != 8 + key_len)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, 4 + key_len);
		pTask->length = sizeof(ProtoHeader);
		return  EINVAL;
	}
	key = pTask->data + sizeof(ProtoHeader) + 4;
	inc = buff2int(pTask->data + sizeof(ProtoHeader) + 4 + key_len);

	value_len = sizeof(value) - 1;
	result = db_inc(g_db_list[group_id], key, key_len, inc, \
			value, &value_len);
	if (result == 0)
	{
		fdht_binlog_write(FDHT_OP_TYPE_SOURCE_SET, key, key_len, \
				value, value_len);

		pTask->length = sizeof(ProtoHeader) + 4 + value_len;
		int2buff(value_len, pTask->data + sizeof(ProtoHeader));
		memcpy(pTask->data+sizeof(ProtoHeader)+4, value, value_len);
	}
	else
	{
		pTask->length = sizeof(ProtoHeader);
	}

	return result;
}

