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
#include "ini_file_reader.h"
#include "fdht_types.h"
#include "fdht_proto.h"
#include "fdht_func.h"
#include "fdht_client.h"

GroupArray g_group_array = {NULL, 0};

int fdht_client_init(const char *filename)
{
	char *pBasePath;
	IniItemInfo *items;
	int nItemCount;
	int result;

	if ((result=iniLoadItems(filename, &items, &nItemCount)) != 0)
	{
		logError("load conf file \"%s\" fail, ret code: %d", \
			filename, result);
		return result;
	}

	//iniPrintItems(items, nItemCount);

	while (1)
	{
		pBasePath = iniGetStrValue("base_path", items, nItemCount);
		if (pBasePath == NULL)
		{
			logError("conf file \"%s\" must have item " \
				"\"base_path\"!", filename);
			result = ENOENT;
			break;
		}

		snprintf(g_base_path, sizeof(g_base_path), "%s", pBasePath);
		chopPath(g_base_path);
		if (!fileExists(g_base_path))
		{
			logError("\"%s\" can't be accessed, error info: %s", \
				g_base_path, strerror(errno));
			result = errno != 0 ? errno : ENOENT;
			break;
		}
		if (!isDir(g_base_path))
		{
			logError("\"%s\" is not a directory!", g_base_path);
			result = ENOTDIR;
			break;
		}

		g_network_timeout = iniGetIntValue("network_timeout", \
				items, nItemCount, DEFAULT_NETWORK_TIMEOUT);
		if (g_network_timeout <= 0)
		{
			g_network_timeout = DEFAULT_NETWORK_TIMEOUT;
		}

		if ((result=fdht_load_groups(items, nItemCount, \
				&g_group_array)) != 0)
		{
			break;
		}

#ifdef __DEBUG__
		fprintf(stderr, "base_path=%s, " \
			"network_timeout=%d, "\
			"group_count=%d\n", \
			g_base_path, g_network_timeout, \
			g_group_array.count);
#endif

		break;
	}

	iniFreeItems(items);

	return result;
}

void fdht_client_destroy()
{
	fdht_free_group_array(&g_group_array);
}

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

FDHTServerInfo *get_readable_connection(ServerArray *pServerArray, \
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

int fdht_get(const char *pKey, const int key_len, \
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

	group_id = PJWHash(pKey, key_len) % g_group_array.count;
	pServer = get_readable_connection(g_group_array.groups + group_id, \
                	&new_connection, &result);
	if (pServer == NULL)
	{
		return result;
	}

	printf("group_id=%d\n", group_id);

	memset(&header, 0, sizeof(header));
	header.cmd = FDHT_PROTO_CMD_GET;
	int2buff(group_id, header.group_id);
	int2buff(4 + key_len, header.pkg_len);

	in_buff = NULL;
	while (1)
	{
		if ((result=tcpsenddata(pServer->sock, &header, \
			sizeof(header), g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		int2buff(key_len, buff);
		if ((result=tcpsenddata(pServer->sock, buff, 4, \
			g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		if ((result=tcpsenddata(pServer->sock, (char *)pKey, key_len, \
			g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		if ((result=fdht_recv_response(pServer, &in_buff, \
			0, &in_bytes)) != 0)
		{
			logError("recv data from server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		if (in_bytes < 4)
		{
			logError("server %s:%d reponse bytes: %d < 4", \
				pServer->ip_addr, pServer->port, in_bytes);
			result = EINVAL;
			break;
		}

		if (*ppValue != NULL)
		{
			if (in_bytes - 4 >= *value_len)
			{
				*value_len = 0;
				result = ENOSPC;
				break;
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
				logError("malloc %d bytes fail, " \
					"errno: %d, error info: %s", \
					*value_len + 1, errno, strerror(errno));
				result = errno != 0 ? errno : ENOMEM;
				break;
			}
		}

		memcpy(*ppValue, in_buff + 4, *value_len);
		*(*ppValue + *value_len) = '\0';
		break;
	}

	if (new_connection)
	{
		fdht_quit(pServer);
		fdht_disconnect_server(pServer);
	}

	if (in_buff != NULL)
	{
		free(in_buff);
	}

	return result;
}

int fdht_set(const char *pKey, const int key_len, \
	const char *pValue, const int value_len)
{
	int result;
	ProtoHeader header;
	char buff[16];
	int in_bytes;
	int group_id;
	FDHTServerInfo *pServer;
	bool new_connection;

	group_id = PJWHash(pKey, key_len) % g_group_array.count;
	pServer = get_writable_connection(g_group_array.groups + group_id, \
                	&new_connection, &result);
	if (pServer == NULL)
	{
		return result;
	}

	printf("group_id=%d\n", group_id);

	memset(&header, 0, sizeof(header));
	header.cmd = FDHT_PROTO_CMD_SET;
	int2buff(group_id, header.group_id);
	int2buff(8 + key_len + value_len, header.pkg_len);

	while (1)
	{
		if ((result=tcpsenddata(pServer->sock, &header, \
			sizeof(header), g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		int2buff(key_len, buff);
		if ((result=tcpsenddata(pServer->sock, buff, 4, \
			g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		if ((result=tcpsenddata(pServer->sock, (char *)pKey, key_len, \
			g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		int2buff(value_len, buff);
		if ((result=tcpsenddata(pServer->sock, buff, 4, \
			g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		if ((result=tcpsenddata(pServer->sock, (char *)pValue, value_len, \
			g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		if ((result=fdht_recv_header(pServer, &in_bytes)) != 0)
		{
			logError("recv data from server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		if (in_bytes != 0)
		{
			logError("server %s:%d reponse bytes: %d != 0", \
				pServer->ip_addr, pServer->port, in_bytes);
			result = EINVAL;
			break;
		}

		break;
	}

	if (new_connection)
	{
		fdht_quit(pServer);
		fdht_disconnect_server(pServer);
	}

	return result;
}

int fdht_inc(const char *pKey, const int key_len, \
		const int increase, char *pValue, int *value_len)
{
	int result;
	ProtoHeader header;
	char buff[32];
	char *in_buff;
	int in_bytes;
	int group_id;
	FDHTServerInfo *pServer;
	bool new_connection;

	printf("g_group_array.count=%d\n", g_group_array.count);

	group_id = PJWHash(pKey, key_len) % g_group_array.count;
	pServer = get_writable_connection(g_group_array.groups + group_id, \
                	&new_connection, &result);
	if (pServer == NULL)
	{
		return result;
	}

	printf("group_id=%d\n", group_id);

	memset(&header, 0, sizeof(header));
	header.cmd = FDHT_PROTO_CMD_INC;
	int2buff(group_id, header.group_id);
	int2buff(8 + key_len, header.pkg_len);

	while (1)
	{
		if ((result=tcpsenddata(pServer->sock, &header, \
			sizeof(header), g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		int2buff(key_len, buff);
		if ((result=tcpsenddata(pServer->sock, buff, 4, \
			g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		if ((result=tcpsenddata(pServer->sock, (char *)pKey, key_len, \
			g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		int2buff(increase, buff);
		if ((result=tcpsenddata(pServer->sock, buff, 4, \
			g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		in_buff = buff;
		if ((result=fdht_recv_response(pServer, &in_buff, \
			sizeof(buff), &in_bytes)) != 0)
		{
			logError("recv data from server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		if (in_bytes < 4)
		{
			logError("server %s:%d reponse bytes: %d < 4!", \
				pServer->ip_addr, pServer->port, in_bytes);
			result = EINVAL;
			break;
		}

		if (in_bytes - 4 >= *value_len)
		{
			*value_len = 0;
			result = ENOSPC;
			break;
		}

		*value_len = in_bytes - 4;
		memcpy(pValue, in_buff + 4, *value_len);
		*(pValue + (*value_len)) = '\0';
		break;
	}

	if (new_connection)
	{
		fdht_quit(pServer);
		fdht_disconnect_server(pServer);
	}

	return result;
}

int fdht_delete(const char *pKey, const int key_len)
{
	int result;
	ProtoHeader header;
	char buff[16];
	int in_bytes;
	int group_id;
	FDHTServerInfo *pServer;
	bool new_connection;

	group_id = PJWHash(pKey, key_len) % g_group_array.count;
	pServer = get_writable_connection(g_group_array.groups + group_id, \
                	&new_connection, &result);
	if (pServer == NULL)
	{
		return result;
	}

	printf("group_id=%d\n", group_id);

	memset(&header, 0, sizeof(header));
	header.cmd = FDHT_PROTO_CMD_DEL;
	int2buff(group_id, header.group_id);
	int2buff(4 + key_len, header.pkg_len);

	while (1)
	{
		if ((result=tcpsenddata(pServer->sock, &header, \
			sizeof(header), g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		int2buff(key_len, buff);
		if ((result=tcpsenddata(pServer->sock, buff, 4, \
			g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		if ((result=tcpsenddata(pServer->sock, (char *)pKey, key_len, \
			g_network_timeout)) != 0)
		{
			logError("send data to server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		if ((result=fdht_recv_header(pServer, &in_bytes)) != 0)
		{
			logError("recv data from server %s:%d fail, " \
				"errno: %d, error info: %s", \
				pServer->ip_addr, pServer->port, \
				result, strerror(result));
			break;
		}

		if (in_bytes != 0)
		{
			logError("server %s:%d reponse bytes: %d != 0", \
				pServer->ip_addr, pServer->port, in_bytes);
			result = EINVAL;
			break;
		}

		break;
	}

	if (new_connection)
	{
		fdht_quit(pServer);
		fdht_disconnect_server(pServer);
	}

	return result;
}

