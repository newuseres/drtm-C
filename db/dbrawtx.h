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

#ifndef DBRAWTX_H
#define DBRAWTX_H

#include <stdlib.h>
#include <string.h>
#include "memstore/rawtables.h"

typedef struct DBRAWTX {
    RAWTables *txdb_;
} DBRAWTX;

DBRAWTX* DBRAWTX_new(RAWTables* tables) {
    DBRAWTX* dbrawtx = (DBRAWTX*) malloc(sizeof(DBRAWTX));
    dbrawtx->txdb_ = tables;
    return dbrawtx;
}

void DBRAWTX_free(DBRAWTX* dbrawtx) {
    free(dbrawtx);
}

void Begin(DBRAWTX* dbrawtx) {
}

char Abort(DBRAWTX* dbrawtx) {
    return 0;
}

char End(DBRAWTX* dbrawtx) {
    return 1;
}

void Add(DBRAWTX* dbrawtx, int tableid, uint64_t key, uint64_t* val) {  
    Put(dbrawtx->txdb_, tableid, key, val);    
}

void Add(DBRAWTX* dbrawtx, int tableid, uint64_t key, uint64_t* val, int len) {
    char* value = (char*) malloc(len);
    memcpy(value, val, len);
    Put(dbrawtx->txdb_, tableid, key, (uint64_t *)value);
//    free(value);
}

void Delete(DBRAWTX* dbrawtx, int tableid, uint64_t key) {
    Delete(dbrawtx->txdb_, tableid, key);
}

RAWStore_Iterator* GetRawIterator(DBRAWTX* dbrawtx, int tableid) {
    return GetIterator(dbrawtx->txdb_, tableid);
}

char Get(DBRAWTX* dbrawtx, int tableid, uint64_t key, uint64_t** val) {
    *val = Get(dbrawtx->txdb_, tableid, key);
    return *val != NULL;
}

uint64_t* GetRecord(DBRAWTX* dbrawtx, int tableid, uint64_t key) {
    return dGet(dbrawtx->txdb_, tableid, key);
}


#endif