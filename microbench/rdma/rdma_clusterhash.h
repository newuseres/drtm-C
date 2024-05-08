/*
 *  The code is part of our project called DrTM, which leverages HTM and RDMA
 * for speedy distributed in-memory transactions.
 *
 *
 * Copyright (C) 2015 Institute of Parallel and Distributed Systems (IPADS),
 * Shanghai Jiao Tong University All rights reserved.
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

#ifndef RDMAHASHEXT_H
#define RDMAHASHEXT_H

typedef unsigned long long uint64_t;
typedef unsigned int uint32_t;

#include <iostream>
#include <stdlib.h>

#define HASH_LOCK 0

// paddings for RDMA,may not be needed
#define MAX_THREADS 16

#define CLUSTER_H 8

inline uint64_t MurmurHash64A(uint64_t key, unsigned int seed) {

    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^ (8 * m);
    const uint64_t *data = &key;
    const uint64_t *end = data + 1;

    while (data != end) {
        uint64_t k = *data++;
        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
    }

    const unsigned char *data2 = (const unsigned char *)data;

    switch (8 & 7) {
    case 7:
        h ^= uint64_t(data2[6]) << 48;
    case 6:
        h ^= uint64_t(data2[5]) << 40;
    case 5:
        h ^= uint64_t(data2[4]) << 32;
    case 4:
        h ^= uint64_t(data2[3]) << 24;
    case 3:
        h ^= uint64_t(data2[2]) << 16;
    case 2:
        h ^= uint64_t(data2[1]) << 8;
    case 1:
        h ^= uint64_t(data2[0]);
        h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

struct HeaderNode {
    uint64_t next;
    uint64_t keys[CLUSTER_H];
    uint64_t indexes[CLUSTER_H];
};
struct DataNode {
    uint64_t key;
    bool valid;
};

struct RdmaClusterHash {

    char *array;
    uint64_t length;
    uint64_t Logical_length;
    uint64_t indirect_length;
    uint64_t total_length;
    uint64_t entrysize;
    uint64_t header_size;
    uint64_t data_size;
    uint64_t size;
    uint64_t free_indirect;
    uint64_t free_data;
};

RdmaClusterHash *RdmaClusterHash_new(uint64_t esize, uint64_t len, char *arr) {
    RdmaClusterHash *it = (RdmaClusterHash *)malloc(sizeof(RdmaClusterHash));
    it->entrysize = (((esize - 1) >> 3) + 1) << 3;

    it->length = len;
    it->Logical_length = it->length * 1 / CLUSTER_H;
    it->indirect_length = it->length * 1 / 2;
    it->total_length = it->length + it->indirect_length;
    it->free_indirect = it->Logical_length;
    it->free_data = it->indirect_length;
    it->header_size = sizeof(HeaderNode);
    it->data_size = (sizeof(DataNode) + it->entrysize);
    it->size =
        it->indirect_length * it->header_size + it->length * it->data_size;
    it->array = arr;
}

void RdmaClusterHash_free(RdmaClusterHash *it) {}

uint64_t GetHash(RdmaClusterHash *it, uint64_t key) {
    return MurmurHash64A(key, 0xdeadbeef) % it->Logical_length;
}

DataNode *getDataNode(RdmaClusterHash *it, uint64_t i) {
    return (DataNode *)(it->array + getDataNode_loc(it, i));
}
uint64_t getDataNode_loc(RdmaClusterHash *it, uint64_t i) {
    return it->indirect_length * it->header_size +
           (i - it->indirect_length) * it->data_size;
}
uint64_t getHeaderNode_loc(RdmaClusterHash *it, uint64_t i) {
    return i * it->header_size;
}
void Insert(RdmaClusterHash *it, uint64_t key, void *val) {
    if (it->free_data == it->total_length) {
        printf("fail when inserting %d\n", key);
        //    assert(false);
    }
    uint64_t hash = GetHash(it, key);
    HeaderNode *node = (HeaderNode *)(it->array + hash * it->header_size);
    while (node->next != 0) {
        node = (HeaderNode *)(it->array + (node->next) * it->header_size);
    }
    for (int i = 0; i < CLUSTER_H; i++) {
        if (node->indexes[i] == 0) {
            DataNode *free_node = getDataNode(it, it->free_data);
            free_node->key = key;
            memcpy((void *)(free_node + 1), val, it->entrysize);
            node->keys[i] = key;
            node->indexes[i] = it->free_data;
            it->free_data++;
            return;
        }
    }
    if (it->free_indirect == it->indirect_length) {
        printf("fail when allocating indirect node,key is %d\n", key);
        //    assert(false);
    }
    node->next = it->free_indirect;
    node = (HeaderNode *)(it->array + it->free_indirect * it->header_size);
    it->free_indirect++;
    DataNode *free_node = getDataNode(it, it->free_data);
    free_node->key = key;
    memcpy((void *)(free_node + 1), val, it->entrysize);
    node->keys[0] = key;
    node->indexes[0] = it->free_data;
    it->free_data++;
    return;
}

uint64_t *Get(RdmaClusterHash *it, uint64_t key) {
    uint64_t hash = GetHash(it, key);
    HeaderNode *node = (HeaderNode *)(it->array + hash * it->header_size);
    while (true) {
        for (int i = 0; i < CLUSTER_H; i++) {
            if (node->keys[i] == key && node->indexes[i] != 0) {
                DataNode *datanode = getDataNode(it, node->indexes[i]);
                return (uint64_t *)(datanode + 1);
            }
        }
        if (node->next != NULL)
            node = (HeaderNode *)(it->array + (node->next) * it->header_size);
        else {
            //    assert(false);
        }
    }
}

uint64_t read(RdmaClusterHash *it, uint64_t key) {
    uint64_t hash = GetHash(it, key);
    HeaderNode *node = (HeaderNode *)(it->array + hash * it->header_size);
    int count = 0;
    while (true) {
        count++;
        for (int i = 0; i < CLUSTER_H; i++) {
            if (node->keys[i] == key && node->indexes[i] != 0) {
                DataNode *datanode = getDataNode(it, node->indexes[i]);
                return count;
            }
        }
        if (node->next != NULL)
            node = (HeaderNode *)(it->array + (node->next) * it->header_size);
        else {
            // assert(false);
        }
    }
}

void *Delete(RdmaClusterHash *it, uint64_t key) {
    // TODO
    return NULL;
}
#endif
