//func.c

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
#include "fdht_func.h"
#include "func.h"

DBInfo **g_db_list = NULL;
int g_db_count = 0;

#define DB_FILE_PREFIX_MAX_SIZE  32

static int fdht_load_from_conf_file(const char *filename, char *bind_addr, \
		const int addr_size, int **group_ids, int *group_count, \
		DBType *db_type, u_int64_t *nCacheSize, \
		char *db_file_prefix)
{
	char *pBasePath;
	char *pBindAddr;
	char *pDbType;
	char *pDbFilePrefix;
	char *pRunByGroup;
	char *pRunByUser;
	char *pCacheSize;
	char *pMaxPkgSize;
	IniItemInfo *items;
	int nItemCount;
	int result;
	u_int64_t max_pkg_size;

	if ((result=iniLoadItems(filename, &items, &nItemCount)) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"load conf file \"%s\" fail, ret code: %d", \
			__LINE__, filename, result);
		return result;
	}

	//iniPrintItems(items, nItemCount);

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

		load_log_level(items, nItemCount);
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
					FDHT_SERVER_DEFAULT_PORT);
		if (g_server_port <= 0)
		{
			g_server_port = FDHT_SERVER_DEFAULT_PORT;
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
				items, nItemCount, DEFAULT_MAX_CONNECTONS);
		if (g_max_connections <= 0)
		{
			g_max_connections = DEFAULT_MAX_CONNECTONS;
		}
		if ((result=set_rlimit(RLIMIT_NOFILE, g_max_connections)) != 0)
		{
			break;
		}

		g_max_threads = iniGetIntValue("max_threads", \
				items, nItemCount, DEFAULT_MAX_CONNECTONS);
		if (g_max_threads <= 0)
		{
			g_max_threads = DEFAULT_MAX_CONNECTONS;
		}

		pMaxPkgSize = iniGetStrValue("max_pkg_size", \
				items, nItemCount);
		if (pMaxPkgSize == NULL)
		{
			g_max_pkg_size = FDHT_MAX_PKG_SIZE;
		}
		else 
		{
			if ((result=parse_bytes(pMaxPkgSize, \
					&max_pkg_size)) != 0)
			{
				return result;
			}
			g_max_pkg_size = (int)max_pkg_size;
		}

		g_sync_wait_usec = iniGetIntValue("sync_wait_msec", \
			 items, nItemCount, DEFAULT_SYNC_WAIT_MSEC);
		if (g_sync_wait_usec <= 0)
		{
			g_sync_wait_usec = DEFAULT_SYNC_WAIT_MSEC;
		}
		g_sync_wait_usec *= 1000;

		
		pRunByGroup = iniGetStrValue("run_by_group", \
						items, nItemCount);
		pRunByUser = iniGetStrValue("run_by_user", \
						items, nItemCount);
		if ((result=set_run_by(pRunByGroup, pRunByUser)) != 0)
		{
			break;
		}

		if ((result=load_allow_hosts(items, nItemCount, \
                	 &g_allow_ip_addrs, &g_allow_ip_count)) != 0)
		{
			break;
		}

		pDbType = iniGetStrValue("db_type", items, nItemCount);
		if (pDbType == NULL)
		{
			*db_type = DB_BTREE;
		}
		else if (strcasecmp(pDbType, "btree") == 0) 
		{
			*db_type = DB_BTREE;
		}
		else if (strcasecmp(pDbType, "hash") == 0) 
		{
			*db_type = DB_HASH;
		}
		else
		{
			logError("file: "__FILE__", line: %d, " \
				"item \"db_type\" is invalid, value: \"%s\"", \
				__LINE__, pDbType);
			result = EINVAL;
			break;
		}

		pCacheSize = iniGetStrValue("cache_size", items, nItemCount);
		if (pCacheSize == NULL)
		{
			*nCacheSize = 16 * 1024 * 1024;
		}
		else if ((result=parse_bytes(pCacheSize, nCacheSize)) != 0)
		{
			break;
		}

		pDbFilePrefix = iniGetStrValue("db_prefix", items, nItemCount);
		if (pDbFilePrefix == NULL || *pDbFilePrefix == '\0')
		{
			logError("file: "__FILE__", line: %d, " \
				"item \"db_prefix\" not exist or is empty!", \
				__LINE__);
			result = ENOENT;
			break;
		}
		snprintf(db_file_prefix, DB_FILE_PREFIX_MAX_SIZE, \
			"%s", pDbFilePrefix);

		if ((result=load_group_ids(items, nItemCount, "group_ids", \
			group_ids, group_count)) != 0)
		{
			break;
		}

		logInfo("FastDHT v%d.%d, base_path=%s, " \
			"group count=%d, " \
			"network_timeout=%d, "\
			"port=%d, bind_addr=%s, " \
			"max_connections=%d, "    \
			"max_threads=%d, "    \
			"max_pkg_size=%d KB, " \
			"db_type=%s, " \
			"db_prefix=%s, " \
			"cache_size=%d MB, sync_wait_msec=%dms, "  \
			"allow_ip_count=%d", \
			g_version.major, g_version.minor, \
			g_base_path, *group_count, \
			g_network_timeout, \
			g_server_port, bind_addr, g_max_connections, \
			g_max_threads, g_max_pkg_size / 1024, \
			*db_type == DB_BTREE ? "btree" : "hash", \
			db_file_prefix, (int)(*nCacheSize / (1024 * 1024)), \
			g_sync_wait_usec / 1000, g_allow_ip_count);

		break;
	}

	iniFreeItems(items);

	return result;
}

int fdht_func_init(const char *filename, char *bind_addr, \
		const int addr_size)
{
	int result;
	int *group_ids;
	int group_count;
	int *pGroupId;
	int *pGroupEnd;
	int max_group_id;
	int i;
	DBType db_type;
	u_int64_t nCacheSize;
	char db_file_prefix[DB_FILE_PREFIX_MAX_SIZE];
	char db_filename[DB_FILE_PREFIX_MAX_SIZE+8];

	result = fdht_load_from_conf_file(filename, bind_addr, \
		addr_size, &group_ids, &group_count, &db_type, \
		&nCacheSize, db_file_prefix);
	if (result != 0)
	{
		return result;
	}

	max_group_id = 0;
	pGroupEnd = group_ids + group_count;
	for (pGroupId=group_ids; pGroupId<pGroupEnd; pGroupId++)
	{
		if (*pGroupId > max_group_id)
		{
			max_group_id = *pGroupId;
		}
	}

	g_db_count = max_group_id + 1;
	g_db_list = (DBInfo **)malloc(sizeof(DBInfo *) * g_db_count);
	if (g_db_list == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, sizeof(DBInfo *) * g_db_count, \
			errno, strerror(errno));
		free(group_ids);
		return errno != 0 ? errno : ENOMEM;
	}

	for (i=0; i<g_db_count; i++)
	{
		g_db_list[i] = NULL;
	}

	result = 0;
	for (pGroupId=group_ids; pGroupId<pGroupEnd; pGroupId++)
	{
		g_db_list[*pGroupId] = (DBInfo *)malloc(sizeof(DBInfo));
		if (g_db_list[*pGroupId] == NULL)
		{
			result = errno != 0 ? errno : ENOMEM;
			logError("file: "__FILE__", line: %d, " \
				"malloc %d bytes fail, " \
				"errno: %d, error info: %s", \
				__LINE__, sizeof(DBInfo), \
				result, strerror(result));
			break;
		}

		snprintf(db_filename, sizeof(db_filename), "%s%03d", \
			db_file_prefix, *pGroupId);
		if ((result=db_init(g_db_list[*pGroupId], db_type, nCacheSize, \
				g_base_path, db_filename)) != 0)
		{
			break;
		}
	}

	free(group_ids);
	return result;
}

void fdht_func_destroy()
{
	int i;

	for (i=0; i<g_db_count; i++)
	{
		if (g_db_list[i] != NULL)
		{
			db_destroy(g_db_list[i]);
			free(g_db_list[i]);
			g_db_list[i] = NULL;
		}
	}
}

int fdht_write_to_fd(int fd, get_filename_func filename_func, \
		const void *pArg, const char *buff, const int len)
{
	if (ftruncate(fd, 0) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"truncate file \"%s\" to empty fail, " \
			"error no: %d, error info: %s", \
			__LINE__, filename_func(pArg, NULL), \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	if (lseek(fd, 0, SEEK_SET) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"rewind file \"%s\" to start fail, " \
			"error no: %d, error info: %s", \
			__LINE__, filename_func(pArg, NULL), \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	if (write(fd, buff, len) != len)
	{
		logError("file: "__FILE__", line: %d, " \
			"write to file \"%s\" fail, " \
			"error no: %d, error info: %s", \
			__LINE__, filename_func(pArg, NULL), \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	if (fsync(fd) != 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"sync file \"%s\" to disk fail, " \
			"error no: %d, error info: %s", \
			__LINE__, filename_func(pArg, NULL), \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOENT;
	}

	return 0;
}

