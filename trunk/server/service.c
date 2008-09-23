//service.c

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include "logger.h"
#include "sockopt.h"
#include "shared_func.h"
#include "ini_file_reader.h"
#include "fdht_global.h"
#include "global.h"
#include "service.h"

int fdht_load_from_conf_file(const char *filename, char *bind_addr, \
		const int addr_size, int **group_ids, int *group_count)
{
	char *pBasePath;
	char *pBindAddr;
	char *pRunByGroup;
	char *pRunByUser;
	IniItemInfo *items;
	int nItemCount;
	int result;

	if ((result=iniLoadItems(filename, &items, &nItemCount)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"load conf file \"%s\" fail, ret code: %d", \
			__LINE__, filename, result);
		return result;
	}

	while (1)
	{
		if (iniGetBoolValue("disabled", items, nItemCount))
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\" disabled=true, exit", \
				__LINE__, filename);
			result = ECANCELED;
			break;
		}

		pBasePath = iniGetStrValue("base_path", items, nItemCount);
		if (pBasePath == NULL)
		{
			logError("file: "__FILE__", line: %d, " \
				"conf file \"%s\" must have item " \
				"\"base_path\"!", \
				__LINE__, filename);
			result = ENOENT;
			break;
		}

		snprintf(g_base_path, sizeof(g_base_path), "%s", pBasePath);
		chopPath(g_base_path);
		if (!fileExists(g_base_path))
		{
			logError("file: "__FILE__", line: %d, " \
				"\"%s\" can't be accessed, error info: %s", \
				__LINE__, strerror(errno), g_base_path);
			result = errno != 0 ? errno : ENOENT;
			break;
		}
		if (!isDir(g_base_path))
		{
			logError("file: "__FILE__", line: %d, " \
				"\"%s\" is not a directory!", \
				__LINE__, g_base_path);
			result = ENOTDIR;
			break;
		}

		fdfs_load_log_level(items, nItemCount);
		if ((result=log_init("fdhtd")) != 0)
		{
			break;
		}

		g_network_timeout = iniGetIntValue("network_timeout", \
				items, nItemCount, DEFAULT_NETWORK_TIMEOUT);
		if (g_network_timeout <= 0)
		{
			g_network_timeout = DEFAULT_NETWORK_TIMEOUT;
		}

		g_server_port = iniGetIntValue("port", items, nItemCount, \
					FDFS_STORAGE_SERVER_DEF_PORT);
		if (g_server_port <= 0)
		{
			g_server_port = FDFS_STORAGE_SERVER_DEF_PORT;
		}

		pBindAddr = iniGetStrValue("bind_addr", items, nItemCount);
		if (pBindAddr == NULL)
		{
			bind_addr[0] = '\0';
		}
		else
		{
			snprintf(bind_addr, addr_size, "%s", pBindAddr);
		}

		g_max_connections = iniGetIntValue("max_connections", \
				items, nItemCount, FDFS_DEF_MAX_CONNECTONS);
		if (g_max_connections <= 0)
		{
			g_max_connections = FDFS_DEF_MAX_CONNECTONS;
		}
		if ((result=set_rlimit(RLIMIT_NOFILE, g_max_connections)) != 0)
		{
			break;
		}

		g_max_threads = iniGetIntValue("max_threads", \
				items, nItemCount, FDFS_DEF_MAX_CONNECTONS);
		if (g_max_threads <= 0)
		{
			g_max_threads = FDFS_DEF_MAX_CONNECTONS;
		}
		
		pRunByGroup = iniGetStrValue("run_by_group", \
						items, nItemCount);
		pRunByUser = iniGetStrValue("run_by_user", \
						items, nItemCount);
		if ((result=set_run_by(pRunByGroup, pRunByUser)) != 0)
		{
			return result;
		}

		if ((result=fdfs_load_allow_hosts(items, nItemCount, \
                	 &g_allow_ip_addrs, &g_allow_ip_count)) != 0)
		{
			return result;
		}

		logInfo("FastDHT v%d.%d, base_path=%s, " \
			"group count=%d, " \
			"network_timeout=%d, "\
			"port=%d, bind_addr=%s, " \
			"max_connections=%d, "    \
			"max_threads=%d, "    \
			"allow_ip_count=%d", \
			g_version.major, g_version.minor, \
			g_base_path, *group_count, \
			g_network_timeout, \
			g_server_port, bind_addr, g_max_connections, \
			g_max_threads, g_allow_ip_count);

		break;
	}

	iniFreeItems(items);

	return result;
}

