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

#define SYNC_REQ_WAIT_SECONDS	60

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
	int result;

	switch(((ProtoHeader *)pTask->data)->cmd)
	{
		case FDHT_PROTO_CMD_GET:
			result = deal_cmd_get(pTask);
			break;
		case FDHT_PROTO_CMD_SET:
			result = deal_cmd_set(pTask, \
					FDHT_OP_TYPE_SOURCE_SET);
			break;
		case FDHT_PROTO_CMD_SYNC_SET:
			result = deal_cmd_set(pTask, \
					FDHT_OP_TYPE_REPLICA_SET);
			break;
		case FDHT_PROTO_CMD_INC:
			result = deal_cmd_inc(pTask);
			break;
		case FDHT_PROTO_CMD_DEL:
			result = deal_cmd_del(pTask, \
					FDHT_OP_TYPE_SOURCE_DEL);
			break;
		case FDHT_PROTO_CMD_SYNC_DEL:
			result = deal_cmd_del(pTask, \
					FDHT_OP_TYPE_REPLICA_DEL);
			break;
		case FDHT_PROTO_CMD_HEART_BEAT:
			pTask->length = sizeof(ProtoHeader);
			result = 0;
			break;
		case FDHT_PROTO_CMD_QUIT:
			close(pTask->ev.ev_fd);
			free_queue_push(pTask);
			return 0;
		case FDHT_PROTO_CMD_SYNC_REQ:
			result = deal_cmd_sync_req(pTask);
			break;
		case FDHT_PROTO_CMD_SYNC_NOTIFY:
			result = deal_cmd_sync_done(pTask);
			break;
		default:
			logError("file: "__FILE__", line: %d, " \
				"client ip: %s, invalid cmd: 0x%02X", \
				__LINE__, pTask->client_ip, \
				((ProtoHeader *)pTask->data)->cmd);

			pTask->length = sizeof(ProtoHeader);
			result = EINVAL;
			break;
	}

	pHeader = (ProtoHeader *)pTask->data;
	//printf("client ip: %s, cmd=%d, resp pkg_len=%d\n", pTask->client_ip, pHeader->cmd, pTask->length - sizeof(ProtoHeader));

	pHeader->status = result;
	int2buff((int)time(NULL), pHeader->timestamp);
	pHeader->cmd = FDHT_PROTO_CMD_RESP;
	int2buff(pTask->length - sizeof(ProtoHeader), pHeader->pkg_len);

	send_queue_push(pTask);

	return 0;
}

#define CHECK_GROUP_ID(pTask, key_hash_code, group_id, timestamp, expires) \
	key_hash_code = buff2int(((ProtoHeader *)pTask->data)->key_hash_code); \
	timestamp = buff2int(((ProtoHeader *)pTask->data)->timestamp); \
	expires = buff2int(((ProtoHeader *)pTask->data)->expires); \
	if (timestamp > 0) \
	{ \
		if (expires > 0)  \
		{ \
			expires = time(NULL) + (expires - timestamp); \
		} \
	} \
	group_id = key_hash_code % g_group_count; \
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

#define PARSE_COMMON_BODY(min_body_len, pTask, nInBodyLen, key_info, \
		pNameSpace, pObjectId, pKey) \
	nInBodyLen = pTask->length - sizeof(ProtoHeader); \
	if (nInBodyLen <= min_body_len) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d <= %d", \
			__LINE__, pTask->client_ip, nInBodyLen, min_body_len); \
		pTask->length = sizeof(ProtoHeader); \
		return EINVAL; \
	} \
 \
	key_info.namespace_len = buff2int(pTask->data + sizeof(ProtoHeader)); \
	if (key_info.namespace_len < 0 || \
		key_info.namespace_len > FDHT_MAX_NAMESPACE_LEN) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid namespace length: %d", \
			__LINE__, pTask->client_ip, key_info.namespace_len); \
		pTask->length = sizeof(ProtoHeader); \
		return EINVAL; \
	} \
	if (nInBodyLen <= min_body_len + key_info.namespace_len) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d <= %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, min_body_len + key_info.namespace_len); \
		pTask->length = sizeof(ProtoHeader); \
		return EINVAL; \
	} \
	pNameSpace = pTask->data + sizeof(ProtoHeader) + 4; \
	if (key_info.namespace_len > 0) \
	{ \
		memcpy(key_info.szNameSpace,pNameSpace,key_info.namespace_len);\
	} \
 \
	key_info.obj_id_len = buff2int(pNameSpace + key_info.namespace_len); \
	if (key_info.obj_id_len < 0 || \
		key_info.obj_id_len > FDHT_MAX_OBJECT_ID_LEN) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid object length: %d", \
			__LINE__, pTask->client_ip, key_info.obj_id_len); \
		pTask->length = sizeof(ProtoHeader); \
		return EINVAL; \
	} \
	if (nInBodyLen <= min_body_len + key_info.namespace_len + \
			key_info.obj_id_len) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d <= %d", \
			__LINE__, pTask->client_ip, nInBodyLen, \
			min_body_len + key_info.namespace_len + \
			key_info.obj_id_len); \
		pTask->length = sizeof(ProtoHeader); \
		return EINVAL; \
	} \
	pObjectId = pNameSpace + key_info.namespace_len + 4; \
	if (key_info.obj_id_len > 0) \
	{ \
		memcpy(key_info.szObjectId, pObjectId, key_info.obj_id_len); \
	} \
 \
	key_info.key_len = buff2int(pObjectId + key_info.obj_id_len); \
	if (key_info.key_len < 0 || key_info.key_len > FDHT_MAX_SUB_KEY_LEN) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, invalid key length: %d", \
			__LINE__, pTask->client_ip, key_info.key_len); \
		pTask->length = sizeof(ProtoHeader); \
		return EINVAL; \
	} \
	if (nInBodyLen < min_body_len + key_info.namespace_len + \
			key_info.obj_id_len + key_info.key_len) \
	{ \
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, min_body_len + key_info.namespace_len + \
			key_info.obj_id_len + key_info.key_len); \
		pTask->length = sizeof(ProtoHeader); \
		return EINVAL; \
	} \
	pKey = pObjectId + key_info.obj_id_len + 4; \
	memcpy(key_info.szKey, pKey, key_info.key_len); \


/**
* request body format:
*       key_info.namespace_len:  4 bytes big endian integer
*       namespace: can be emtpy
*       key_info.obj_id_len:  4 bytes big endian integer
*       object_id: the object id (can be empty)
*       key_info.key_len:  4 bytes big endian integer
*       key:      key name
* response body format:
*       value_len:  4 bytes big endian integer
*       value:      value buff
*/
static int deal_cmd_get(struct task_info *pTask)
{
	int nInBodyLen;
	FDHTKeyInfo key_info;
	int key_hash_code;
	int group_id;
	int timestamp;
	int expires;
	char *pNameSpace;
	char *pObjectId;
	char *pKey;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	char *pValue;
	char *p;  //tmp var
	int full_key_len;
	int value_len;
	int result;

	CHECK_GROUP_ID(pTask, key_hash_code, group_id, timestamp, expires)

	PARSE_COMMON_BODY(12, pTask, nInBodyLen, key_info, pNameSpace, \
			pObjectId, pKey)

	if (nInBodyLen != 12 + key_info.namespace_len + key_info.obj_id_len + \
				key_info.key_len)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, 12 + key_info.namespace_len + \
			key_info.obj_id_len + key_info.key_len);
		pTask->length = sizeof(ProtoHeader);
		return EINVAL;
	}

	FDHT_PACK_FULL_KEY(key_info, full_key, full_key_len, p)

	pValue = pTask->data + sizeof(ProtoHeader);
	value_len = pTask->size - sizeof(ProtoHeader);
	if ((result=db_get(g_db_list[group_id], full_key, full_key_len, \
               	&pValue, &value_len)) != 0)
	{
		if (result == ENOSPC)
		{
			char *pTemp;

			pTemp = (char *)pTask->data;
			pTask->data = malloc(sizeof(ProtoHeader) + value_len);
			if (pTask->data == NULL)
			{
				logError("file: "__FILE__", line: %d, " \
					"malloc %d bytes failed, " \
					"errno: %d, error info: %s", \
					__LINE__, pTask->size, \
					errno, strerror(errno));

				pTask->data = pTemp;  //restore old data
				pTask->length = sizeof(ProtoHeader);
				return ENOMEM;
			}

			free(pTemp);
			pTask->size = sizeof(ProtoHeader) + value_len;

			pValue = pTask->data + sizeof(ProtoHeader);
			if ((result=db_get(g_db_list[group_id], full_key, \
				full_key_len, &pValue, &value_len)) != 0)
			{
				pTask->length = sizeof(ProtoHeader);
				return result;
			}
		}
		else
		{
			pTask->length = sizeof(ProtoHeader);
			return result;
		}
	}

	if (expires != FDHT_EXPIRES_NONE)
	{
		int2buff(expires, pValue);
		result = db_set(g_db_list[group_id], full_key, full_key_len, \
			pValue, value_len);
	}

	value_len -= 4;
	pTask->length = sizeof(ProtoHeader) + 4 + value_len;
	memcpy(((ProtoHeader *)pTask->data)->expires, pValue, 4);
	int2buff(value_len, pTask->data+sizeof(ProtoHeader));

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
*       key_info.namespace_len:  4 bytes big endian integer
*       namespace: can be emtpy
*       key_info.obj_id_len:  4 bytes big endian integer
*       object_id: the object id (can be empty)
*       key_info.key_len:  4 bytes big endian integer
*       key:      key name
*       value_len:  4 bytes big endian integer
*       value:      value buff
* response body format:
*      none
*/
static int deal_cmd_set(struct task_info *pTask, byte op_type)
{
	int nInBodyLen;
	FDHTKeyInfo key_info;
	int group_id;
	int key_hash_code;
	time_t timestamp;
	time_t expires;
	char *pNameSpace;
	char *pObjectId;
	char *pKey;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	int full_key_len;
	char *p;  //tmp var
	char *pValue;
	int value_len;
	int result;

	CHECK_GROUP_ID(pTask, key_hash_code, group_id, timestamp, expires)

	PARSE_COMMON_BODY(16, pTask, nInBodyLen, key_info, pNameSpace, \
			pObjectId, pKey)

	value_len = buff2int(pKey + key_info.key_len);
	if (value_len < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, value length: %d < 0", \
			__LINE__, pTask->client_ip, value_len);
		pTask->length = sizeof(ProtoHeader);
		return  EINVAL;
	}
	if (nInBodyLen != 16 + key_info.namespace_len + key_info.obj_id_len + \
			key_info.key_len + value_len)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, 16 + key_info.namespace_len + \
			key_info.obj_id_len + key_info.key_len + value_len);
		pTask->length = sizeof(ProtoHeader);
		return  EINVAL;
	}
	pValue = pKey + key_info.key_len;

	if (op_type == FDHT_OP_TYPE_SOURCE_SET)
	{
		timestamp = time(NULL);
	}

	int2buff(expires, pValue);
	value_len += 4;

	pTask->length = sizeof(ProtoHeader);

	FDHT_PACK_FULL_KEY(key_info, full_key, full_key_len, p)

	result = db_set(g_db_list[group_id], full_key, full_key_len, \
			pValue, value_len);
	if (result == 0)
	{
		memcpy(((ProtoHeader *)pTask->data)->expires, pValue, 4);

		fdht_binlog_write(timestamp, op_type, key_hash_code, expires, \
				&key_info, pValue+4, value_len-4);
	}

	return result;
}

/**
* request body format:
*       namespace_len:  4 bytes big endian integer
*       namespace: can be emtpy
*       obj_id_len:  4 bytes big endian integer
*       object_id: the object id (can be empty)
*       key_len:  4 bytes big endian integer
*       key:      key name
* response body format:
*      none
*/
static int deal_cmd_del(struct task_info *pTask, byte op_type)
{
	int nInBodyLen;
	FDHTKeyInfo key_info;
	int key_hash_code;
	int group_id;
	int timestamp;
	int expires;
	char *pNameSpace;
	char *pObjectId;
	char *pKey;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	int full_key_len;
	int result;
	char *p;

	CHECK_GROUP_ID(pTask, key_hash_code, group_id, timestamp, expires)

	PARSE_COMMON_BODY(12, pTask, nInBodyLen, key_info, pNameSpace, \
			pObjectId, pKey)

	if (nInBodyLen != 12 + key_info.namespace_len + key_info.obj_id_len + \
			key_info.key_len)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, 12 + key_info.namespace_len + \
			key_info.obj_id_len + key_info.key_len);
		pTask->length = sizeof(ProtoHeader);
		return  EINVAL;
	}

	FDHT_PACK_FULL_KEY(key_info, full_key, full_key_len, p)

	pTask->length = sizeof(ProtoHeader);
	result = db_delete(g_db_list[group_id], full_key, full_key_len);
	if (result == 0)
	{
		if (op_type == FDHT_OP_TYPE_SOURCE_DEL)
		{
			timestamp = time(NULL);
		}
		fdht_binlog_write(timestamp, op_type, key_hash_code, expires, \
				&key_info, NULL, 0);
	}

	return result;
}

/**
* request body format:
*       key_info.namespace_len:  4 bytes big endian integer
*       namespace: can be emtpy
*       key_info.obj_id_len:  4 bytes big endian integer
*       object_id: the object id (can be empty)
*       key_info.key_len:  4 bytes big endian integer
*       key:      key name
*       incr      4 bytes big endian integer
* response body format:
*      value_len: 4 bytes big endian integer
*      value :  value_len bytes
*/
static int deal_cmd_inc(struct task_info *pTask)
{
	int nInBodyLen;
	FDHTKeyInfo key_info;
	int key_hash_code;
	int group_id;
	int timestamp;
	time_t expires;
	char *pNameSpace;
	char *pObjectId;
	char *pKey;
	char full_key[FDHT_MAX_FULL_KEY_LEN];
	char value[32];
	int full_key_len;
	int value_len;
	int inc;
	char *p;  //tmp var
	int result;

	CHECK_GROUP_ID(pTask, key_hash_code, group_id, timestamp, expires)

	PARSE_COMMON_BODY(16, pTask, nInBodyLen, key_info, pNameSpace, \
			pObjectId, pKey)
	if (nInBodyLen != 16 + key_info.namespace_len + key_info.obj_id_len + \
			key_info.key_len)
	{
		logError("file: "__FILE__", line: %d, " \
			"client ip: %s, body length: %d != %d", \
			__LINE__, pTask->client_ip, \
			nInBodyLen, 16 + key_info.namespace_len + \
			key_info.obj_id_len + key_info.key_len);
		pTask->length = sizeof(ProtoHeader);
		return  EINVAL;
	}

	inc = buff2int(pKey + key_info.key_len);

	FDHT_PACK_FULL_KEY(key_info, full_key, full_key_len, p)

	value_len = sizeof(value) - 1;
	result = db_inc_ex(g_db_list[group_id], full_key, full_key_len, inc, \
			value, &value_len, expires);
	if (result == 0)
	{
		value_len -= 4;  //skip expires
		expires = (time_t)buff2int(value);
		fdht_binlog_write(time(NULL), FDHT_OP_TYPE_SOURCE_SET, \
				key_hash_code, expires, &key_info, \
				value+4, value_len);

		pTask->length = sizeof(ProtoHeader) + 4 + value_len;
		int2buff(value_len, pTask->data + sizeof(ProtoHeader));
		memcpy(((ProtoHeader *)pTask->data)->expires, value, 4);
		memcpy(pTask->data+sizeof(ProtoHeader)+4, value+4, value_len);
	}
	else
	{
		pTask->length = sizeof(ProtoHeader);
	}

	return result;
}

