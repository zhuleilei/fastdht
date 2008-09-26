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

#define FDHT_PROTO_CMD_QUIT	19
#define FDHT_PROTO_CMD_SET	11
#define FDHT_PROTO_CMD_INC	12
#define FDHT_PROTO_CMD_GET	13
#define FDHT_PROTO_CMD_DEL	14
#define FDHT_PROTO_CMD_RESP     20

#define FDHT_PROTO_PKG_LEN_SIZE		4
#define FDHT_PROTO_CMD_SIZE		1

typedef int fdht_pkg_size_t;

typedef struct
{
	char pkg_len[FDHT_PROTO_PKG_LEN_SIZE];
	char group_id[FDHT_PROTO_PKG_LEN_SIZE]; //the group id key belong to
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

#ifdef __cplusplus
}
#endif

#endif

