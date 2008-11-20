/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//fdht_proto.h

#ifndef _FDHT_PROTO_H_
#define _FDHT_PROTO_H_

#include "fdht_types.h"

#define FDHT_PROTO_CMD_QUIT	10

#define FDHT_PROTO_CMD_SET	11
#define FDHT_PROTO_CMD_INC	12
#define FDHT_PROTO_CMD_GET	13
#define FDHT_PROTO_CMD_DEL	14

#define FDHT_PROTO_CMD_SYNC_REQ	   21
#define FDHT_PROTO_CMD_SYNC_NOTIFY 22  //sync done notify
#define FDHT_PROTO_CMD_SYNC_SET	   23
#define FDHT_PROTO_CMD_SYNC_DEL	   24

#define FDHT_PROTO_CMD_HEART_BEAT  30

#define FDHT_PROTO_CMD_RESP        40

#define FDHT_PROTO_PKG_LEN_SIZE		4
#define FDHT_PROTO_CMD_SIZE		1

typedef int fdht_pkg_size_t;

typedef struct
{
	char pkg_len[FDHT_PROTO_PKG_LEN_SIZE];  //body length
	char group_id[FDHT_PROTO_PKG_LEN_SIZE]; //the group id key belong to
	char timestamp[FDHT_PROTO_PKG_LEN_SIZE]; //current time
	char timeout[FDHT_PROTO_PKG_LEN_SIZE];   //remain timeout
	char cmd;
	char status;
} ProtoHeader;

#ifdef __cplusplus
extern "C" {
#endif

int fdht_recv_header(FDHTServerInfo *pServer, fdht_pkg_size_t *in_bytes);

int fdht_recv_response(FDHTServerInfo *pServer, \
		char **buff, const int buff_size, \
		fdht_pkg_size_t *in_bytes);
int fdht_quit(FDHTServerInfo *pServer);

/**
* connect to the server
* params:
*	pServer: server
* return: 0 success, !=0 fail, return the error code
**/
int fdht_connect_server(FDHTServerInfo *pServer);

/**
* close connection to the server
* params:
*	pServer: server
* return:
**/
void fdht_disconnect_server(FDHTServerInfo *pServer);

int fdht_client_set(FDHTServerInfo *pServer, const time_t timestamp, \
	const int prot_cmd, const int group_id, \
	const char *pKey, const int key_len, \
	const char *pValue, const int value_len);

int fdht_client_delete(FDHTServerInfo *pServer, const time_t timestamp, \
	const int prot_cmd, const int group_id, \
	const char *pKey, const int key_len);

int fdht_client_heart_beat(FDHTServerInfo *pServer);

#ifdef __cplusplus
}
#endif

#endif

