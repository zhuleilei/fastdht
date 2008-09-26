/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//db_op.h

#ifndef _DB_OP_H
#define _DB_OP_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <db.h>
#include "fdht_define.h"

typedef DBTYPE DBType;

typedef struct
{
	DB_ENV *env;
	DB *db;
} DBInfo;

#ifdef __cplusplus
extern "C" {
#endif

int db_init(DBInfo *pDBInfo, const DBType type, const u_int64_t nCacheSize, \
	const char *base_path, const char *filename);
int db_destroy(DBInfo *pDBInfo);

#ifdef __cplusplus
}
#endif

#endif

