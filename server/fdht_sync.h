/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdht_sync.h

#ifndef _FDHT_SYNC_H_
#define _FDHT_SYNC_H_

#include "fdht_types.h"
#include "fdht_proto.h"

#define FDHT_OP_TYPE_SOURCE_SET		'S'
#define FDHT_OP_TYPE_SOURCE_DEL		'D'
#define FDHT_OP_TYPE_REPLICA_SET	's'
#define FDHT_OP_TYPE_REPLICA_DEL	'd'

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
	char *data;
	int size;
	int length;
} BinField;

typedef struct
{
	char ip_addr[IP_ADDRESS_SIZE];
	bool need_sync_old;
	bool sync_old_done;
	time_t until_timestamp;
	int mark_fd;
	int binlog_index;
	int binlog_fd;
	off_t binlog_offset;
	int64_t scan_row_count;
	int64_t sync_row_count;
	int64_t last_write_row_count;
} BinLogReader;

typedef struct
{
	time_t timestamp;
	char op_type;
	BinField key;
	BinField value;
	//time_t expire;  //key expire, 0 for never expired
} BinLogRecord;

extern int g_binlog_fd;
extern int g_binlog_index;

extern int g_fdht_sync_thread_count;

int fdht_sync_init();
int fdht_sync_destroy();
int fdht_binlog_write(const char op_type, const BinField *pKey, \
		const BinField *pValue);

int fdht_sync_thread_start(const FDHTServerInfo *pStorage);
int kill_fdht_sync_threads();

#ifdef __cplusplus
}
#endif

#endif
