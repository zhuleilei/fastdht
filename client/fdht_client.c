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
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include "fdfs_base64.h"
#include "fdht_global.h"
#include "sockopt.h"
#include "logger.h"
#include "hash.h"
#include "shared_func.h"
#include "fdht_types.h"
#include "fdht_proto.h"
#include "fdht_client.h"

FDHTServerInfo *get_writable_connection(ServerArray *pServerArray, \
		bool *new_connection, int *err_no)
{
	FDHTServerInfo *pServer;
	FDHTServerInfo *pEnd;

	pEnd = pServerArray->servers + pServerArray->count;
	for (pServer = pServerArray->servers; pServer<pEnd; pServer++)
	{
		if (pServer->sock > 0)  //already connected
		{
			*new_connection = false;
			return pServer;
		}

		if (fdht_connect_server(pServer) == 0)
		{
			*new_connection = true;
			return pServer;
		}
	}

	*err_no = ENOENT;
	return NULL;
}

FDHTServerInfo *get_readabl_connection(ServerArray *pServerArray, \
		bool *new_connection, int *err_no)
{
	FDHTServerInfo *pServer;
	FDHTServerInfo *pEnd;

	if (pServerArray->read_index >= pServerArray->count)
	{
		pServerArray->read_index = 0;
	}

	pEnd = pServerArray->servers + pServerArray->count;
	for (pServer = pServerArray->servers + pServerArray->read_index; \
		pServer<pEnd; pServer++)
	{
		if (pServer->sock > 0)  //already connected
		{
			*new_connection = false;
			pServerArray->read_index++;
			return pServer;
		}

		if (fdht_connect_server(pServer) == 0)
		{
			*new_connection = true;
			pServerArray->read_index++;
			return pServer;
		}
	}

	pEnd = pServerArray->servers + pServerArray->read_index;	
	for (pServer = pServerArray->servers; pServer<pEnd; pServer++)
	{
		if (pServer->sock > 0)  //already connected
		{
			*new_connection = false;
			pServerArray->read_index++;
			return pServer;
		}

		if (fdht_connect_server(pServer) == 0)
		{
			*new_connection = true;
			pServerArray->read_index++;
			return pServer;
		}
	}

	*err_no = ENOENT;
	return NULL;
}

int fdht_get(GroupArray *pGroupArray, const char *pKey, const int key_len, \
		char **ppValue, int *value_len)
{
	int result;
	ProtoHeader header;
	char buff[16];
	char *in_buff;
	int in_bytes;
	int group_id;
	FDHTServerInfo *pServer;
	bool new_connection;

	group_id = PJWHash(pKey, key_len) % pGroupArray->count;
	pServer = get_readabl_connection(pGroupArray->groups + group_id, \
                	&new_connection, &result);
	if (pServer == NULL)
	{
		return result;
	}

	memset(&header, 0, sizeof(header));
	header.cmd = FDHT_PROTO_CMD_GET;
	int2buff(group_id, header.group_id);
	int2buff(4 + key_len, header.pkg_len);

	if ((result=tcpsenddata(pServer->sock, &header, \
			sizeof(header), g_network_timeout)) != 0)
	{
		logError("send data to server %s:%d fail, " \
			"errno: %d, error info: %s", \
			pServer->ip_addr, \
			pServer->port, \
			result, strerror(result));
		return result;
	}

	int2buff(key_len, buff);
	if ((result=tcpsenddata(pServer->sock, buff, 4, \
		g_network_timeout)) != 0)
	{
		logError("send data to server %s:%d fail, " \
			"errno: %d, error info: %s", \
			pServer->ip_addr, \
			pServer->port, \
			result, strerror(result));
		return result;
	}

	if ((result=tcpsenddata(pServer->sock, (char *)pKey, key_len, \
		g_network_timeout)) != 0)
	{
		logError("send data to server %s:%d fail, " \
			"errno: %d, error info: %s", \
			pServer->ip_addr, pServer->port, \
			result, strerror(result));
		return result;
	}

	in_buff = NULL;
	if ((result=fdht_recv_response(pServer, &in_buff, \
		0, &in_bytes)) != 0)
	{
		logError("recv data from server %s:%d fail, " \
			"errno: %d, error info: %s", \
			pServer->ip_addr, pServer->port, \
			result, strerror(result));
		return result;
	}

	if (in_bytes < 4)
	{
		logError("server %s:%d reponse bytes: %d < 4", \
			pServer->ip_addr, pServer->port, in_bytes);
		return EINVAL;
	}

	if (*ppValue != NULL)
	{
		if (in_bytes - 4 >= *value_len)
		{
			*value_len = 0;
			free(in_buff);
			return ENOSPC;
		}

		*value_len = in_bytes - 4;
	}
	else
	{
		*value_len = in_bytes - 4;
		*ppValue = (char *)malloc(*value_len + 1);
		if (*ppValue == NULL)
		{
			*value_len = 0;
			free(in_buff);
			logError("malloc %d bytes fail, " \
				"errno: %d, error info: %s", \
				*value_len + 1, errno, strerror(errno));
			return errno != 0 ? errno : ENOMEM;
		}
	}

	memcpy(*ppValue, in_buff + 4, *value_len);
	*(*ppValue + *value_len) = '\0';

	return 0;
}

int fdht_set(GroupArray *pGroupArray, const char *pKey, const int key_len, \
	const char *pValue, const int value_len)
{
	return 0;
}

int fdht_delete(GroupArray *pGroupArray, const char *pKey, const int key_len)
{
	return 0;
}

