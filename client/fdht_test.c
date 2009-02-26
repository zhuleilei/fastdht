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
#include "fdht_func.h"

int main(int argc, char *argv[])
{
	char *conf_filename;
	int result;
	int expires;
	FDHTKeyInfo key_info;
	int conn_success_count;
	int conn_fail_count;
	char szValue[256];
	int value_len;

	printf("This is FastDHT client test program v%d.%d\n" \
"\nCopyright (C) 2008, Happy Fish / YuQing\n" \
"\nFastDHT may be copied only under the terms of the GNU General\n" \
"Public License V3, which may be found in the FastDHT source kit.\n" \
"Please visit the FastDHT Home Page http://www.csource.org/ \n" \
"for more detail.\n\n" \
, g_version.major, g_version.minor);

	if (argc < 2)
	{
		printf("Usage: %s <config_file>\n", argv[0]);
		return 1;
	}

	conf_filename = argv[1];

	g_log_level = LOG_DEBUG;
	if ((result=fdht_client_init(conf_filename)) != 0)
	{
		return result;
	}

	//g_keep_alive = true;
	if (g_keep_alive)
	{
		if ((result=fdht_connect_all_servers(&g_group_array, true, \
			&conn_success_count, &conn_fail_count)) != 0)
		{
			printf("fdht_connect_all_servers fail, " \
				"error code: %d, error info: %s\n", \
				result, strerror(result));
			return result;
		}
	}

	srand(time(NULL));

	expires = time(NULL) + 3600;
	memset(&key_info, 0, sizeof(key_info));
	key_info.namespace_len = sprintf(key_info.szNameSpace, "bbs");
	key_info.obj_id_len = sprintf(key_info.szObjectId, "test");
	key_info.key_len = sprintf(key_info.szKey, "reg");

	/*
	key_info.obj_id_len = sprintf(key_info.szObjectId, "o%d", 12345678);
	key_info.key_len = sprintf(key_info.szKey, "k%d", 978654);
	*/

	while (1)
	{
		char *value;

		//memset(szValue, '1', sizeof(szValue));
		value_len = sprintf(szValue, "%d", rand());

		printf("original value=%s(%d)\n", szValue, value_len);

		if ((result=fdht_set(&key_info, expires, szValue, value_len)) != 0)
		{
			break;
		}

		value_len = sizeof(szValue);
		if ((result=fdht_inc(&key_info, expires, 100, \
				szValue, &value_len)) != 0)
		{
			break;
		}

		printf("value_len: %d\n", value_len);
		printf("value: %s\n", szValue);

		value = szValue;
		value_len = sizeof(szValue);
		if ((result=fdht_get_ex(&key_info, expires, &value, &value_len)) != 0)
		{
			printf("result=%d\n", result);
			break;
		}

		printf("value_len: %d\n", value_len);
		printf("value: %s\n", value);
		/*
		if ((result=fdht_delete(&key_info)) != 0)
		{
			break;
		}
		*/
		break;
	}

	if (g_keep_alive)
	{
		fdht_disconnect_all_servers(&g_group_array);
	}
	
	fdht_client_destroy();

	return result;
}

