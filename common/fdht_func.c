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

int load_group_ids(IniItemInfo *items, const int nItemCount, \
		const char *item_name, int **group_ids, int *group_count)
{
	char *pGroupIds;
	char *pBuff;
	char *pNumStart;
	char *p;
	int alloc_count;
	int result;
	int count;
	int i;
	char ch;
	char *pNumStart1;
	char *pNumStart2;
	int nNumLen1;
	int nNumLen2;
	int nStart;
	int nEnd;

	if ((pGroupIds=iniGetStrValue(item_name, items, nItemCount)) == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"no item \"%s\" in conf file", \
			__LINE__, item_name);

		*group_count = 0;
		*group_ids = NULL;
		return ENOENT;
	}

	alloc_count = getOccurCount(pGroupIds, ',') + 1;
	*group_count = 0;
	*group_ids = (int *)malloc(sizeof(int) * alloc_count);
	if (*group_ids == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"malloc %d bytes fail, errno: %d, error info: %s.", \
			__LINE__, sizeof(int) * alloc_count, \
			errno, strerror(errno));
		return errno != 0 ? errno : ENOMEM;
	}

	pBuff = strdup(pGroupIds);
	if (pBuff == NULL)
	{
		logError("file: "__FILE__", line: %d, " \
			"strdup \"%s\" fail, errno: %d, error info: %s.", \
			__LINE__, pGroupIds, \
			errno, strerror(errno));
		free(*group_ids);
		*group_ids = NULL;
		return errno != 0 ? errno : ENOMEM;
	}

	result = 0;
	p = pBuff;
	while (*p != '\0')
	{
		while (*p == ' ' || *p == '\t') //trim prior spaces
		{
			p++;
		}

		if (*p == '\0')
		{
			break;
		}

		if (*p >= '0' && *p <= '9')
		{
			pNumStart = p;
			p++;
			while (*p >= '0' && *p <= '9')
			{
				p++;
			}

			if (p - pNumStart == 0)
			{
				logError("file: "__FILE__", line: %d, " \
					"invalid group ids \"%s\", " \
					"which contains empty group id!", \
					__LINE__, pGroupIds);
				result = EINVAL;
				break;
			}

			ch = *p;
			if (!(ch == ','  || ch == '\0'))
			{
				logError("file: "__FILE__", line: %d, " \
					"invalid group ids \"%s\", which contains " \
					"invalid char: %c(0x%02X)! remain string: %s", \
					__LINE__, pGroupIds, *p, *p, p);
				result = EINVAL;
				break;
			}

			*p = '\0';
			(*group_ids)[*group_count] = atoi(pNumStart);
			(*group_count)++;
	
			if (ch == '\0')
			{
				break;
			}

			p++;  //skip ,
			continue;
		}

		if (*p != '[')
		{
			logError("file: "__FILE__", line: %d, " \
				"invalid group ids \"%s\", which contains " \
				"invalid char: %c(0x%02X)! remain string: %s", \
				__LINE__, pGroupIds, *p, *p, p);
			result = EINVAL;
			break;
		}

		p++;  //skip [
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
		if (nNumLen1 == 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"invalid group ids: %s, " \
				"empty entry before char %c(0x%02X), " \
				"remain string: %s", \
				__LINE__, pGroupIds, *p, *p, p);
			result = EINVAL;
			break;
		}

		while (*p == ' ' || *p == '\t') //trim spaces
		{
			p++;
		}

		if (*p != '-')
		{
			logError("file: "__FILE__", line: %d, " \
				"expect \"-\", but char %c(0x%02X) ocurs " \
				"in group ids: %s, remain string: %s",\
				__LINE__, *p, *p, pGroupIds, p);
			result = EINVAL;
			break;
		}

		*(pNumStart1 + nNumLen1) = '\0';
		nStart = atoi(pNumStart1);

		p++;  //skip -
		while (*p == ' ' || *p == '\t') //trim spaces
		{
			p++;
		}

		pNumStart2 = p;
		while (*p >='0' && *p <= '9')
		{
			p++;
		}

		nNumLen2 = p - pNumStart2;
		if (nNumLen2 == 0)
		{
			logError("file: "__FILE__", line: %d, " \
				"invalid group ids: %s, " \
				"empty entry before char %c(0x%02X)", \
				__LINE__, pGroupIds, *p, *p);
			result = EINVAL;
			break;
		}

		/* trim tail spaces */
		while (*p == ' ' || *p == '\t')
		{
			p++;
		}

		if (*p != ']')
		{
			logError("file: "__FILE__", line: %d, " \
				"expect \"]\", but char %c(0x%02X) ocurs " \
				"in group ids: %s",\
				__LINE__, *p, *p, pGroupIds);
			result = EINVAL;
			break;
		}

		*(pNumStart2 + nNumLen2) = '\0';
		nEnd = atoi(pNumStart2);

		count = nEnd - nStart;
		if (count < 0)
		{
			count = 0;
		}
		if (alloc_count < *group_count + (count + 1))
		{
			alloc_count += count + 1;
			*group_ids = (int *)realloc(*group_ids, \
				sizeof(int) * alloc_count);
			if (*group_ids == NULL)
			{
				result = errno != 0 ? errno : ENOMEM;
				logError("file: "__FILE__", line: %d, "\
					"malloc %d bytes fail, " \
					"errno: %d, error info: %s.", \
					__LINE__, \
					sizeof(int) * alloc_count,\
					result, strerror(result));

				break;
			}
		}

		for (i=nStart; i<=nEnd; i++)
		{
			(*group_ids)[*group_count] = i;
			(*group_count)++;
		}

		p++; //skip ]
		/* trim spaces */
		while (*p == ' ' || *p == '\t')
		{
			p++;
		}
		if (*p == ',')
		{
			p++; //skip ,
		}
	}

	free(pBuff);

	if (result == 0 && *group_count == 0)
	{
		logError("file: "__FILE__", line: %d, " \
			"invalid group ids count: 0!", __LINE__);
		result = EINVAL;
	}

	if (result != 0)
	{
		*group_count = 0;
		free(*group_ids);
		*group_ids = NULL;
	}

	printf("*group_count=%d\n", *group_count);
	for (i=0; i<*group_count; i++)
	{
		printf("%d\n", (*group_ids)[i]);
	}

	return result;
}

