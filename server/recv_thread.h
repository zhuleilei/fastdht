/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//recv_thread.h

#ifndef _RECV_THREAD_H
#define _RECV_THREAD_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <event.h>
#include "fdht_define.h"
#include "task_queue.h"

#ifdef __cplusplus
extern "C" {
#endif

int recv_notify_write();
int kill_recv_thread();
void *recv_thread_entrance(void* arg);

int recv_process_init(int server_sock);
int recv_add_event(struct task_info *pTask);

#ifdef __cplusplus
}
#endif

#endif

