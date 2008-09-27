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
#include "fdht_client.h"

int main(int argc, char *argv[])
{
	//char *conf_filename;
	int result;
	int key_len;
	char key[32];
	char *value;
	int value_len;
	GroupArray groupArray;

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


	groupArray.count = 1;
	groupArray.groups = (ServerArray *)malloc(sizeof(ServerArray) * \
					groupArray.count);
	if (groupArray.groups == NULL)
	{
		logError("malloc %d bytes fail, errno: %d, error info: %s", \
			sizeof(ServerArray) * groupArray.count, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}
	
	groupArray.groups[0].count = 1;
	groupArray.groups[0].read_index = 0;
	groupArray.groups[0].servers = (FDHTServerInfo *)malloc( \
			sizeof(FDHTServerInfo) * groupArray.groups[0].count);
	if (groupArray.groups[0].servers == NULL)
	{
		logError("malloc %d bytes fail, errno: %d, error info: %s", \
			sizeof(FDHTServerInfo) * groupArray.groups[0].count, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	memset(groupArray.groups[0].servers, 0, sizeof(FDHTServerInfo) * \
			groupArray.groups[0].count);

	strcpy(groupArray.groups[0].servers[0].ip_addr, "127.0.0.1");
	groupArray.groups[0].servers[0].port = 11411;
	groupArray.groups[0].servers[0].sock = -1;

	strcpy(key, "test");
	key_len = strlen(key);

	/*
	value = "1234567890122";
	value_len = strlen(value);

	if ((result=fdht_set(&groupArray, key, key_len, \
		value, value_len)) != 0)
	{
		return result;
	}
	*/

	if ((result=fdht_inc(&groupArray, key, key_len, 100)) != 0)
	{
		return result;
	}

	value = NULL;
	if ((result=fdht_get(&groupArray, key, key_len, \
		&value, &value_len)) != 0)
	{
		return result;
	}

	printf("value_len: %d\n", value_len);
	printf("value: %s\n", value);
	free(value);

	/*
	if ((result=fdht_delete(&groupArray, key, key_len)) != 0)
	{
		return result;
	}
	*/

	return 0;
}

