/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

//task_queue.h

#ifndef _TASK_QUEUE_H
#define _TASK_QUEUE_H 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <event.h>
#include "fdht_define.h"
#include "chain.h"

#define TASK_STATUS_NONE	   0
#define TASK_STATUS_RECV_DONE     11
#define TASK_STATUS_RECV_TIMEOUT  12
#define TASK_STATUS_RECV_ERR	  13
#define TASK_STATUS_SEND_DONE     21
#define TASK_STATUS_SEND_TIMEOUT  22
#define TASK_STATUS_SEND_ERR      23

#define TASK_STATUS_RECV_BASE     10
#define TASK_STATUS_SEND_BASE     20
#define TASK_STATUS_DISCONNECTED  30
#define TASK_STATUS_OUT_MEMERY    40

struct task_info
{
	char status;
	char client_ip[IP_ADDRESS_SIZE];
	struct event ev;
	char *data;
	int size;   //alloc size
	int length; //data length
	int offset; //current offset
	struct task_info *next;
};

struct task_queue_info
{
	struct task_info *head;
	struct task_info *tail;
	pthread_mutex_t lock;
};

#ifdef __cplusplus
extern "C" {
#endif

int task_queue_init();
void task_queue_destroy();

int free_queue_push(struct task_info *pTask);
struct task_info *free_queue_pop();
int free_queue_count();

int recv_queue_push(struct task_info *pTask);
struct task_info *recv_queue_pop();
int recv_queue_count();

int send_queue_push(struct task_info *pTask);
struct task_info *send_queue_pop();
int send_queue_count();

int work_queue_push(struct task_info *pTask);
struct task_info *work_queue_pop();
int work_queue_count();

#ifdef __cplusplus
}
#endif

#endif

