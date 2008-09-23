/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdht_func.c

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
#include "fdht_global.h"
#include "sockopt.h"
#include "shared_func.h"
#include "ini_file_reader.h"
#include "fdht_func.h"

static int fdht_load_allow_hosts(IniItemInfo *items, const int nItemCount, \
		int **group_ids, int *group_count)
{
	int count;
	IniItemInfo *pItem;
	IniItemInfo *pItemStart;
	IniItemInfo *pItemEnd;
	char *pItemValue;
	char *pStart;
	char *pEnd;
	char *p;
	char *pTail;
	int alloc_count;
	int nHeadLen;
	int i;
	in_addr_t addr;
	char hostname[256];

	if ((pItemStart=iniGetValuesEx("allow_hosts", \
		items, nItemCount, &count)) == NULL)
	{
		*allow_ip_count = -1; /* -1 means match any ip address */
		*allow_ip_addrs = NULL;
		return 0;
	}

	pItemEnd = pItemStart + count;
	for (pItem=pItemStart; pItem<pItemEnd; pItem++)
	{
		if (strcmp(pItem->value, "*") == 0)
		{
			*allow_ip_count = -1; /* -1 means match any ip address*/
			*allow_ip_addrs = NULL;
			return 0;
		}
	}

	alloc_count = count;
	*allow_ip_count = 0;
	*allow_ip_addrs = (in_addr_t *)malloc(sizeof(in_addr_t) * alloc_count);
	if (*allow_ip_addrs == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, errno: %d, error info: %s.", \
			__LINE__, sizeof(in_addr_t) * alloc_count, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	for (pItem=pItemStart; pItem<pItemEnd; pItem++)
	{
		if (*(pItem->value) == '\0')
		{
			continue;
		}

		pStart = strchr(pItem->value, '[');
		if (pStart == NULL)
		{
			addr = getIpaddrByName(pItem->value, NULL, 0);
			if (addr == INADDR_NONE)
			{
				logWarning("file: "__FILE__", line: %d, " \
					"invalid host name: %s", \
					__LINE__, pItem->value);
			}
			else
			{
				(*allow_ip_addrs)[*allow_ip_count] = addr;
				(*allow_ip_count)++;
			}

			continue;
		}

		
		pEnd = strchr(pStart, ']');
		if (pEnd == NULL)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"invalid host name: %s, expect \"]\"", \
				__LINE__, pItem->value);
			continue;
		}

		pItemValue = strdup(pItem->value);
		if (pItemValue == NULL)
		{
			logWarning("file: "__FILE__", line: %d, " \
				"strdup fail, " \
				"errno: %d, error info: %s.", \
				__LINE__, errno, strerror(errno));
			continue;
		}

		nHeadLen = pStart - pItem->value;
		pStart = pItemValue + nHeadLen;
		pEnd = pItemValue + (pEnd - pItem->value);
		pTail = pEnd + 1;

		memcpy(hostname, pItem->value, nHeadLen);
		p = pStart + 1;  //skip [

		while (p <= pEnd)
		{
			char *pNumStart1;
			char *pNumStart2;
			int nStart;
			int nEnd;
			int nNumLen1;
			int nNumLen2;
			char end_ch1;
			char end_ch2;
			char szFormat[16];

			while (*p == ' ' || *p == '\t') //trim prior spaces
			{
				p++;
			}

			pNumStart1 = p;
			while (*p >='0' && *p <= '9')
			{
				p++;
			}

			nNumLen1 = p - pNumStart1;
			while (*p == ' ' || *p == '\t') //trim tail spaces
			{
				p++;
			}

			if (!(*p == ',' || *p == '-' || *p == ']'))
			{
				logWarning("file: "__FILE__", line: %d, " \
					"invalid char \"%c\" in host name: %s",\
					__LINE__, *p, pItem->value);
				break;
			}

			end_ch1 = *p;
			*(pNumStart1 + nNumLen1) = '\0';

			if (nNumLen1 == 0)
			{
				logWarning("file: "__FILE__", line: %d, " \
					"invalid host name: %s, " \
					"empty entry before \"%c\"", \
					__LINE__, pItem->value, end_ch1);
				break;
			}

			nStart = atoi(pNumStart1);
			if (end_ch1 == '-')
			{
				p++;   //skip -

				/* trim prior spaces */
				while (*p == ' ' || *p == '\t')
				{
					p++;
				}

				pNumStart2 = p;
				while (*p >='0' && *p <= '9')
				{
					p++;
				}

				nNumLen2 = p - pNumStart2;
				/* trim tail spaces */
				while (*p == ' ' || *p == '\t')
				{
					p++;
				}

				if (!(*p == ',' || *p == ']'))
				{
				logWarning("file: "__FILE__", line: %d, " \
					"invalid char \"%c\" in host name: %s",\
					__LINE__, *p, pItem->value);
				break;
				}

				end_ch2 = *p;
				*(pNumStart2 + nNumLen2) = '\0';

				if (nNumLen2 == 0)
				{
				logWarning("file: "__FILE__", line: %d, " \
					"invalid host name: %s, " \
					"empty entry before \"%c\"", \
					__LINE__, pItem->value, end_ch2);
				break;
				}

				nEnd = atoi(pNumStart2);
			}
			else
			{
				nEnd = nStart;
			}

			if (alloc_count < *allow_ip_count+(nEnd - nStart + 1))
			{
				alloc_count += nEnd - nStart + 1;
				*allow_ip_addrs = (in_addr_t *)realloc(
					*allow_ip_addrs, 
					sizeof(in_addr_t)*alloc_count);
				if (*allow_ip_addrs == NULL)
				{
					logError("file: "__FILE__", line: %d, "\
						"malloc %d bytes fail, " \
						"errno: %d, error info: %s.", \
						__LINE__, \
						sizeof(in_addr_t)*alloc_count,\
						errno, strerror(errno));

					free(pItemValue);
					return errno != 0 ? errno : ENOMEM;
				}
			}

			sprintf(szFormat, "%%0%dd%%s",  nNumLen1);
			for (i=nStart; i<=nEnd; i++)
			{
				sprintf(hostname + nHeadLen, szFormat, \
					i, pTail);

				addr = getIpaddrByName(hostname, NULL, 0);
				if (addr == INADDR_NONE)
				{
				logWarning("file: "__FILE__", line: %d, " \
					"invalid host name: %s", \
					__LINE__, hostname);
				}
				else
				{
					(*allow_ip_addrs)[*allow_ip_count]=addr;
					(*allow_ip_count)++;
				}

			}

			p++;
		}

		free(pItemValue);
	}

	if (*allow_ip_count == 0)
	{
		logWarning("file: "__FILE__", line: %d, " \
			"allow ip count: 0", __LINE__);
	}

	if (*allow_ip_count > 0)
	{
		qsort(*allow_ip_addrs,  *allow_ip_count, sizeof(in_addr_t), \
			cmp_by_ip_addr_t);
	}

	/*
	printf("*allow_ip_count=%d\n", *allow_ip_count);
	for (i=0; i<*allow_ip_count; i++)
	{
		struct in_addr address;
		address.s_addr = (*allow_ip_addrs)[i];
		printf("%s\n", inet_ntoa(address));
	}
	*/

	return 0;
}

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

