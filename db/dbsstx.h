/*
 *  The code is part of our project called DrTM, which leverages HTM and RDMA for speedy distributed
 *  in-memory transactions.
 *
 *
 * Copyright (C) 2015 Institute of Parallel and Distributed Systems (IPADS), Shanghai Jiao Tong University
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  For more about this software, visit:  http://ipads.se.sjtu.edu.cn/drtm.html
 *
 */


#ifndef DRTM_DB_DBSSTX_H
#define DRTM_DB_DBSSTX_H

#include "c_std/string/string.h"
#include <limits.h>

#include "memstore/rawtables.h"
#include "db/network_node.h"
#include "memstore/rdma_resource.h"


#define VALUE_OFFSET 8
#define TIME_OFFSET  0
#define META_LENGTH  8


#define RELEASE_REQ 2
#define WRITE_REQ  1
#define READ_REQ   0

// flag status
#define NONE 0
#define LOCK 1
#define READ_LEASE 2
#define IS_EXPIRED 3
#include "c_std/map/map.h"
#include "c_std/vector/vector.h"


#define _DB_RDMA

#define DEFAULT_INTERVAL 400000   // 0.4ms
#define DELTA 200000 // 0.2ms


#define LOG_SIZE (10 * 1024) //10k for log size


typedef struct msg_struct {
  int tableid;
  int optype;
  uint64_t key;
} msg_struct;

extern uint64_t timestamp;

typedef struct rwset_item{
  int tableid;
  uint64_t key;
  uint64_t loc;
  uint64_t* addr;
  int pid;
  char ro;
} rwset_item;

//实现pair数组
struct pair_int_uint64t{
  int first;
  uint64_t second;
}
int compare_pair_int_uint64t(const pair_int_uint64t a, const pair_int_uint64t b) {
  if(a.first!=b.first) a.first < b.first;
  return a.second < b.second;
}
void pair_int_uint64t_deallocator(void* data) {
    free(data);
}

class DBSSTX_Iterator {

    DBSSTX* sstx_;
    char copyupdate;
    char versioned;
    uint64_t key_;
    RAWStore::Iterator *iter_;
    uint64_t *val_;
    int tableid_;

};
char Valid(DBSSTX_Iterator *this);

uint64_t Key(DBSSTX_Iterator *this);

uint64_t* Value(DBSSTX_Iterator *this);

void Next(DBSSTX_Iterator *this);
// <bound
char Next(DBSSTX_Iterator *this,uint64_t bound);

void Prev(DBSSTX_Iterator *this);
void Seek(DBSSTX_Iterator *this,uint64_t key);

void SeekToFirst(DBSSTX_Iterator *this);

void SeekToLast(DBSSTX_Iterator *this);

void SetUpdate(DBSSTX_Iterator *this){copyupdate = true;}

uint64_t* GetWithSnapshot(DBSSTX_Iterator *this,uint64_t* mn);

  
DBSSTX_Iterator DBSSTX_Iterator_new(DBSSTX* rotx, int tableid, char update);



class DBSSTX {

    // track RW set
    Map* rw_set_type; // map pair_int_uint64t -> rwset_item

    Vector rw_set; //vector* <rwset_item >
    Vector readonly_set; // vector<rwset_item >

    int release_flag = 0;//flag when can free all the remote lock,so it will write back and free memory space

    RAWTables *txdb_;
    RdmaResource *rdma;
    int thread_id;//TODO!!init
    uint64_t localsn;
    uint64_t lastsn; //to check whether another epoch has passed

    //  inline Memstore::MemNode* GetNodeCopy(Memstore::MemNode* org);

    char readonly;
    uint64_t *emptySS[20];
    int emptySSLen;

    //methods for logging
  };


DBSSTX_new(DBSSTX *dbsstx,RAWTables* tables);
DBSSTX_new(DBSSTX *dbsstx,RAWTables* tables,RdmaResource *rdma,int t_id);

DBSSTX_free(DBSSTX *dbsstx);

uint64_t GetSS(DBSSTX *dbsstx);
uint64_t GetMySS(DBSSTX *dbsstx); //return thread's ss
void GetSSDelayP(DBSSTX *dbsstx);
void GetSSDelayN(DBSSTX *dbsstx);
void SSReadLock(DBSSTX *dbsstx);
void SSReadUnLock(DBSSTX *dbsstx);
void UpdateLocalSS(DBSSTX *dbsstx);
void Begin(DBSSTX *dbsstx,char readonly);
char Abort(DBSSTX *dbsstx);
char End(DBSSTX *dbsstx);>





//Copy value
void Add(DBSSTX *dbsstx,int tableid, uint64_t key, uint64_t* val);
char GetLoc(DBSSTX *dbsstx,int tableid, uint64_t key, uint64_t** val);
char Get(DBSSTX *dbsstx,int tableid, uint64_t key, uint64_t** val, char copyupdate,int *_status = NULL);
char GetAt(DBSSTX *dbsstx,int tableid, uint64_t key, uint64_t** val, char copyupdate, uint64_t *loc,int *status = NULL);
void Delete(DBSSTX *dbsstx,int tableid, uint64_t key);

char GetRemote(DBSSTX *dbsstx,int tableid,uint64_t key,String *,Network_Node *node,int pid,uint64_t timestamp);


void chain_travel(DBSSTX *dbsstx,rwset_item &item);
void hashext_travel(DBSSTX *dbsstx,rwset_item &item);
void cuckoo_travel(DBSSTX *dbsstx,rwset_item &item);



//void PrintAllLocks();
//Rdma methods
void PrefetchAllRemote(DBSSTX *dbsstx,uint64_t endtime);
void ReleaseAllRemote(DBSSTX *dbsstx);
void Fallback_LockAll(DBSSTX *dbsstx,SpinLock** sl, int numOfLocks, uint64_t endtime);
void RemoteWriteBack(DBSSTX *dbsstx);

char LockRemote(DBSSTX *dbsstx,rwset_item item,Network_Node *node);
char ReleaseLockRemote(DBSSTX *dbsstx,rwset_item item,Network_Node *node);
char LockLocal(DBSSTX *dbsstx,rwset_item item);
char ReleaseLocal(DBSSTX *dbsstx,rwset_item item);

uint64_t GetLocalLoc(DBSSTX *dbsstx,int tableid,uint64_t key);
uint64_t GetRdmaLoc(DBSSTX *dbsstx,int tableid,uint64_t key,int pid);

void AddToRemoteWriteSet(DBSSTX *dbsstx,int tableid,uint64_t key,int pid,uint64_t *loc = NULL);
void AddToLocalWriteSet(DBSSTX *dbsstx,int tableid,uint64_t key,int pid,uint64_t *loc = NULL);
void AddToRemoteReadSet(DBSSTX *dbsstx,int tableid,uint64_t key,int pid,uint64_t *loc = NULL);
void AddToLocalReadSet(DBSSTX *dbsstx,int tableid,uint64_t key,int pid,uint64_t *loc = NULL);

void AddToLocalReadOnlySet(DBSSTX *dbsstx,int tableid,uint64_t key,uint64_t *loc = NULL);

void ReleaseAllLocal(DBSSTX *dbsstx);
void RdmaFetchAdd (DBSSTX *dbsstx, uint64_t off,int pid,uint64_t value);
//The general lock operation
void Lock(DBSSTX *dbsstx,rwset_item &item);
void Release(DBSSTX *dbsstx,rwset_item &item,char flag = false);
void LocalLockSpin(DBSSTX *dbsstx,char *loc);
void LocalReleaseSpin(DBSSTX *dbsstx,char *loc);


void _LocalLock(DBSSTX *dbsstx,rwset_item &item);
void _LocalRelease(DBSSTX *dbsstx,rwset_item &item);


void GetLease(DBSSTX *dbsstx,rwset_item &item, uint64_t endtime);

char AllLeasesAreValid(DBSSTX *dbsstx);

void GetLocalLease(DBSSTX *dbsstx,int tableid,uint64_t key,uint64_t *loc,uint64_t endtime);

char AllLocalLeasesValid(DBSSTX *dbsstx);

void ClearRwset(DBSSTX *dbsstx);

String HandleMsg(DBSSTX *dbsstx,String &req);

RAWStore_Iterator *GetRawIterator(DBSSTX *dbsstx,int tableid);

DBSSTX_Iterator *GetIterator(DBSSTX *dbsstx,int tableid);

uint64_t * check(DBSSTX *dbsstx,int tableid ,uint64_t key);

void Redo(DBSSTX *dbsstx,uint64_t *value, int32_t delta, char after);
void Redo(DBSSTX *dbsstx,uint64_t *value, float delta, char after);


#endif  //
