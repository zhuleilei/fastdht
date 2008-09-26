/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include "fdht_define.h"
#include "shared_func.h"
#include "logger.h"
#include "sockopt.h"
#include "fdht_global.h"
#include "fdht_types.h"
#include "fdht_proto.h"

int fdht_recv_header(FDHTServerInfo *pServer, fdht_pkg_size_t *in_bytes)
{
	ProtoHeader resp;
	int result;

	if ((result=tcprecvdata(pServer->sock, &resp, \
		sizeof(resp), g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"server: %s:%d, recv data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pServer->ip_addr, \
			pServer->port, \
			result, strerror(result));
		*in_bytes = 0;
		return result;
	}

	if (resp.status != 0)
	{
		*in_bytes = 0;
		return resp.status;
	}

	*in_bytes = buff2int(resp.pkg_len);
	if (*in_bytes < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"server: %s:%d, recv package size "INT64_PRINTF_FORMAT" " \
			"is not correct", \
			__LINE__, pServer->ip_addr, \
			pServer->port, *in_bytes);
		*in_bytes = 0;
		return EINVAL;
	}

	return resp.status;
}

int fdht_recv_response(FDHTServerInfo *pServer, \
		char **buff, const int buff_size, \
		fdht_pkg_size_t *in_bytes)
{
	int result;
	bool bMalloced;

	result = fdht_recv_header(pServer, in_bytes);
	if (result != 0)
	{
		return result;
	}

	if (*in_bytes == 0)
	{
		return 0;
	}

	if (*buff == NULL)
	{
		*buff = (char *)malloc((*in_bytes) + 1);
		if (*buff == NULL)
		{
			*in_bytes = 0;

			logError("file: "__FILE__", line: %d, " \
				"malloc "INT64_PRINTF_FORMAT" bytes fail", \
				__LINE__, (*in_bytes) + 1);
			return errno != 0 ? errno : ENOMEM;
		}

		bMalloced = true;
	}
	else 
	{
		if (*in_bytes > buff_size)
		{
			logError("file: "__FILE__", line: %d, " \
				"server: %s:%d, recv body bytes: "INT64_PRINTF_FORMAT"" \
				" exceed max: %d", \
				__LINE__, pServer->ip_addr, \
				pServer->port, *in_bytes, buff_size);
			*in_bytes = 0;
			return ENOSPC;
		}

		bMalloced = false;
	}

	if ((result=tcprecvdata(pServer->sock, *buff, \
		*in_bytes, g_network_timeout)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server: %s:%d, recv data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pServer->ip_addr, \
			pServer->port, \
			result, strerror(result));
		*in_bytes = 0;
		if (bMalloced)
		{
			free(*buff);
			*buff = NULL;
		}
		return result;
	}

	return 0;
}

int fdht_quit(FDHTServerInfo *pServer)
{
	ProtoHeader header;
	int result;

	memset(&header, 0, sizeof(header));
	header.cmd = FDHT_PROTO_CMD_QUIT;
	result = tcpsenddata(pServer->sock, &header, sizeof(header), \
				g_network_timeout);
	if(result != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"tracker server ip: %s, send data fail, " \
			"errno: %d, error info: %s", \
			__LINE__, pServer->ip_addr, \
			result, strerror(result));
		return result;
	}

	return 0;
}

