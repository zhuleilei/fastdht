/**
* Copyright (C) 2008 Happy Fish / YuQing
*
* FastDFS may be copied only under the terms of the GNU General
* Public License V3, which may be found in the FastDFS source kit.
* Please visit the FastDFS Home Page http://www.csource.org/ for more detail.
**/

#ifndef CHAIN_H
#define CHAIN_H

#include "common_define.h"

#define CHAIN_TYPE_INSERT	0
#define CHAIN_TYPE_APPEND	1
#define CHAIN_TYPE_SORTED	2

typedef struct tagChainNode
{
	void *data;
	struct tagChainNode *next;
} ChainNode;

typedef struct
{
	int type;
	ChainNode *head;
	ChainNode *tail;
	FreeDataFunc freeDataFunc;
	CompareFunc compareFunc;
	bool is_malloc_node;
} ChainList;

#ifdef __cplusplus
extern "C" {
#endif

#define chain_init(pList, type, freeDataFunc, compareFunc) \
	chain_init_ex(pList, type, freeDataFunc, compareFunc, true)

#define addNode(pList, data) addNode_ex(pList, data, NULL)
#define insertNodePrior(pList, data) insertNodePrior_ex(pList, data, NULL)
#define appendNode(pList, data) appendNode_ex(pList, data, NULL)

void chain_init_ex(ChainList *pList, const int type, FreeDataFunc freeDataFunc,\
		CompareFunc compareFunc, const bool bMallocNode);
void chain_destroy(ChainList *pList);

int chain_count(ChainList *pList);

int addNode_ex(ChainList *pList, void *data, ChainNode *pNode);
int insertNodePrior_ex(ChainList *pList, void *data, ChainNode *pNode);
int appendNode_ex(ChainList *pList, void *data, ChainNode *pNode);

void freeChainNode(ChainList *pList, ChainNode *pChainNode);
void deleteNodeEx(ChainList *pList, ChainNode *pPreviousNode, \
		ChainNode *pDeletedNode);
void deleteToNodePrevious(ChainList *pList, ChainNode *pPreviousNode, \
		ChainNode *pDeletedNext);
int deleteOne(ChainList *pList, void *data);
int deleteAll(ChainList *pList, void *data);
void *chain_pop_head(ChainList *pList);

#ifdef __cplusplus
}
#endif

#endif
