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
#include "db_op.h"
#include "fdht_func.h"
#include "service.h"

static DBInfo **db_list = NULL;
static int db_count = 0;

#define DB_FILE_PREFIX_MAX_SIZE  32

int parse_bytes(char *pStr, int64_t *bytes)
{
	char *pReservedEnd;

	pReservedEnd = NULL;
	*bytes = strtol(pStr, &pReservedEnd, 10);
	if (*bytes < 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"bytes: %lld < 0", __LINE__, *bytes);
		return EINVAL;
	}

	if (pReservedEnd == NULL || *pReservedEnd == '\0')
	{
		*bytes *= 1024 * 1024;
	}
	else if (*pReservedEnd == 'G' || *pReservedEnd == 'g')
	{
		*bytes *= 1024 * 1024 * 1024;
	}
	else if (*pReservedEnd == 'M' || *pReservedEnd == 'm')
	{
		*bytes *= 1024 * 1024;
	}
	else if (*pReservedEnd == 'K' || *pReservedEnd == 'k')
	{
		*bytes *= 1024;
	}

	return 0;
}

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
			break;
		}

		if ((result=fdfs_load_allow_hosts(items, nItemCount, \
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
			"db_type=%s, " \
			"db_prefix=%s, " \
			"cache_size=%lld, "    \
			"allow_ip_count=%d", \
			g_version.major, g_version.minor, \
			g_base_path, *group_count, \
			g_network_timeout, \
			g_server_port, bind_addr, g_max_connections, \
			g_max_threads, *db_type == DB_BTREE ? "btree" : "hash", \
			db_file_prefix, *nCacheSize, g_allow_ip_count);

		break;
	}

	iniFreeItems(items);

	return result;
}

int fdht_service_init(const char *filename, char *bind_addr, \
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

	db_count = max_group_id + 1;
	db_list = (DBInfo **)malloc(sizeof(DBInfo *) * db_count);
	if (db_list == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, " \
			"errno: %d, error info: %s", \
			__LINE__, sizeof(DBInfo *) * db_count, \
			errno, strerror(errno));
		free(group_ids);
		return errno != 0 ? errno : ENOMEM;
	}

	for (i=0; i<db_count; i++)
	{
		db_list[i] = NULL;
	}

	result = 0;
	for (pGroupId=group_ids; pGroupId<pGroupEnd; pGroupId++)
	{
		db_list[*pGroupId] = (DBInfo *)malloc(sizeof(DBInfo));
		if (db_list[*pGroupId] == NULL)
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
		if ((result=db_init(db_list[*pGroupId], db_type, nCacheSize, \
				g_base_path, db_filename)) != 0)
		{
			break;
		}
	}

	free(group_ids);
	return result;
}

void fdht_service_destroy()
{
	int i;

	for (i=0; i<db_count; i++)
	{
		if (db_list[i] != NULL)
		{
			db_destroy(db_list[i]);
			free(db_list[i]);
			db_list[i] = NULL;
		}
	}
}

