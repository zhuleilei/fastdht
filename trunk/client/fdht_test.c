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
#include "shared_func.h"
#include "fdht_types.h"
#include "fdht_proto.h"

int main(int argc, char *argv[])
{
	//char *conf_filename;
	int result;
	ProtoHeader header;
	int key_len;
	char key[32];
	char buff[16];
	FDHTServerInfo server;
	FDHTServerInfo *pServer = &server;
	char *in_buff;
	int in_bytes;
	char *value;
	char value_len;

	printf("This is FastDHT client test program v%d.%d\n" \
"\nCopyright (C) 2008, Happy Fish / YuQing\n" \
"\nFastDHT may be copied only under the terms of the GNU General\n" \
"Public License V3, which may be found in the FastDHT source kit.\n" \
"Please visit the FastDHT Home Page http://www.csource.org/ \n" \
"for more detail.\n\n" \
, g_version.major, g_version.minor);

	/*
	if (argc < 3)
	{
		printf("Usage: %s <config_file> <operation>\n" \
			"\toperation: upload, download, getmeta, setmeta " \
			"and delete\n", argv[0]);
		return 1;
	}

	conf_filename = argv[1];
	*/

	
	strcpy(pServer->ip_addr, "127.0.0.1");
	pServer->port = 11411;

	pServer->sock = socket(AF_INET, SOCK_STREAM, 0);
	if(pServer->sock < 0)
	{
		logError("socket create failed, errno: %d, " \
			"error info: %s", errno, strerror(errno));
		return errno != 0 ? errno : EPERM;
	}

	if ((result=connectserverbyip(pServer->sock, pServer->ip_addr, \
			pServer->port)) != 0)
	{
		logError("connect to server %s:%d fail, errno: %d, " \
			"error info: %s", pServer->ip_addr, pServer->port, \
			 result, strerror(result));

		close(pServer->sock);
		return result;
	}

	strcpy(key, "test");
	key_len = strlen(key);

	memset(&header, 0, sizeof(header));
	header.cmd = FDHT_PROTO_CMD_GET;
	int2buff(0, header.group_id);
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
	if ((result=tcpsenddata(pServer->sock, buff, 4, g_network_timeout)) != 0)
	{
		logError("send data to server %s:%d fail, " \
			"errno: %d, error info: %s", \
			pServer->ip_addr, \
			pServer->port, \
			result, strerror(result));
		return result;
	}

	if ((result=tcpsenddata(pServer->sock, key, key_len, g_network_timeout)) != 0)
	{
		logError("send data to server %s:%d fail, " \
			"errno: %d, error info: %s", \
			pServer->ip_addr, pServer->port, \
			result, strerror(result));
		return result;
	}

	in_buff = NULL;
	if ((result=fdht_recv_response(pServer, &in_buff, 0, &in_bytes)) != 0)
	{
		logError("recv data from server %s:%d fail, " \
			"errno: %d, error info: %s", \
			pServer->ip_addr, pServer->port, \
			result, strerror(result));
	}

	value_len = in_bytes - 4;
	value = in_buff + 4;

	if ((result=fdht_quit(pServer)) != 0)
	{
		logError("send data to server %s:%d fail, " \
			"errno: %d, error info: %s", \
			pServer->ip_addr, pServer->port, \
			result, strerror(result));
		return result;
	}

	close(pServer->sock);

	return 0;
}

