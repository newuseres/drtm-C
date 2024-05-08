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

#ifndef RDMAHOPHASH_H
#define RDMAHOPHASH_H

typedef unsigned long long uint64_t;
typedef unsigned int uint32_t;

#include <iostream>
#include <stdlib.h>

#define HASH_LOCK 0

// paddings for RDMA,may not be needed
#define MAX_THREADS 16

#define HOP_H 8

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

struct RdmaHopNode {
    uint64_t keys[HOP_H / 2];
    bool valid[HOP_H / 2];
    uint64_t next;
};

struct RdmaChainNode {
    uint64_t keys[2];
    bool valid[2];
    uint64_t next;
};

struct RdmaHopHash {
    char *array;
    uint64_t length;
    uint64_t hash_length;
    uint64_t chain_length;
    uint64_t entrysize;
    uint64_t size;
    uint64_t free_pointer;
    uint64_t free_chain;
    //    uint64_t free_ptrs[MAX_THREADS];
};

RdmaHopHash *RdmaHopHash_new(uint64_t esize, uint64_t len, char *arr) {
    RdmaHopHash *it = (RdmaHopHash *)malloc(sizeof(RdmaHopHash));
    it->entrysize = (((esize - 1) >> 3) + 1) << 3;
    it->length = len;
    it->hash_length = len / (HOP_H / 2);
    it->chain_length = it->length * 20 / 100;
    it->free_pointer = 1;
    it->size =
        (sizeof(RdmaHopNode) + it->entrysize * HOP_H / 2) * it->hash_length +
        (sizeof(RdmaChainNode) + it->entrysize * 2) * it->chain_length;
    it->free_chain = it->hash_length;
    it->array = arr;
    return it;
}

void RdmaHopHash_free(RdmaHopHash *it) {}

uint64_t get_headernod_loc(RdmaHopHash *it, uint64_t i) {
    //    assert(i < hash_length);
    return (sizeof(RdmaHopNode) + it->entrysize * HOP_H / 2) * i;
}
RdmaHopNode *get_headernod(RdmaHopHash *it, uint64_t i) {
    return (RdmaHopNode *)(it->array + get_headernod_loc(it, i));
}
uint64_t get_chainnod_loc(RdmaHopHash *it, uint64_t i) {
    if (i < it->hash_length) {
        printf("fail when get chainnode loc of %d\n", i);
    }
    //    assert(i >= hash_length);
    return (sizeof(RdmaHopNode) + it->entrysize * HOP_H / 2) * it->hash_length +
           (sizeof(RdmaChainNode) + it->entrysize * 2) * (i - it->hash_length);
}
RdmaChainNode *get_chainnod(RdmaHopHash *it, uint64_t i) {
    return (RdmaChainNode *)(it->array + get_chainnod_loc(it, i));
}

uint64_t GetHash(RdmaHopHash *it, uint64_t key) {
    return MurmurHash64A(key, 0xdeadbeef) % it->hash_length;
    // return key % (_RHASHLENGTH) ;
}

void Insert(RdmaHopHash *it, uint64_t key, void *val) {
    if (it->free_chain >= it->hash_length + it->chain_length) {
        printf("out of memory when inserting %d\n", key);
    }
    //    assert(free_chain < hash_length + chain_length);

    uint64_t hash = GetHash(it, key);
    uint64_t p[2];
    p[0] = hash;
    p[1] = (hash + 1) % it->hash_length;
    for (int i = 0; i < 2; i++) {
        for (int j = 0; j < HOP_H / 2; j++)
            if (get_headernod(it, p[i])->valid[j] == false) {
                get_headernod(it, p[i])->keys[j] = key;
                get_headernod(it, p[i])->valid[j] = true;
                return;
            }
    }

    uint64_t offset = 2;
    uint64_t slot;
    while (true) {
        uint64_t pos = (hash + offset) % it->hash_length;
        bool found = false;
        for (int j = 0; j < HOP_H / 2; j++) {
            if (get_headernod(it, pos)->valid[j] == false) {
                slot = j;
                found = true;
            }
        }
        if (found)
            break;
        else
            offset++;
    }

    // we find empty slot
    while (offset > 1) {
        uint64_t p[2];
        p[0] = (hash + offset - 1) % it->hash_length;
        p[1] = (hash + offset) % it->hash_length;
        bool found = false;
        for (int j = 0; j < HOP_H / 2; j++) {
            if (GetHash(it, get_headernod(it, p[0])->keys[j]) == p[0]) {
                // move this slot to (p[1],slot)
                get_headernod(it, p[1])->keys[slot] =
                    get_headernod(it, p[0])->keys[j];
                get_headernod(it, p[1])->valid[slot] = true;

                get_headernod(it, p[0])->keys[j] = 0;
                get_headernod(it, p[0])->valid[j] = false;
                // current we don't copy data, since it's only a microbench
                slot = j;
                offset--;
                found = true;
                break;
            }
        }
        if (!found) {
            // cannot move to find an empty slot, so use chain instead
            // printf("fail to insert %d\n", key);
            // assert(false);
            if (get_headernod(it, hash)->next == 0) {
                get_headernod(it, hash)->next = it->free_chain;
                get_chainnod(it, it->free_chain)->keys[0] = key;
                get_chainnod(it, it->free_chain)->valid[0] = true;
                it->free_chain++;
                return;
            }
            uint64_t iter = get_headernod(it, hash)->next;
            while (true) {
                for (int i = 0; i < 2; i++) {
                    if (get_chainnod(it, iter)->valid[i] == false) {
                        get_chainnod(it, iter)->keys[i] = key;
                        get_chainnod(it, iter)->valid[i] = true;
                        if (i == 0) {
                            //    assert(false);
                        }
                        return;
                    }
                }
                if (get_chainnod(it, iter)->next == 0)
                    break;
                else
                    iter = get_chainnod(it, iter)->next;
            }

            get_chainnod(it, iter)->next = it->free_chain;
            get_chainnod(it, it->free_chain)->keys[0] = key;
            get_chainnod(it, it->free_chain)->valid[0] = true;
            it->free_chain++;
            return;
        }
        // assert(found);
    }
    get_headernod(it, (hash + 1) % it->hash_length)->keys[slot] = key;
    get_headernod(it, (hash + 1) % it->hash_length)->valid[slot] = true;
    return;
}

uint64_t *Get(RdmaHopHash *it, uint64_t key) { return NULL; }

uint64_t read(RdmaHopHash *it, uint64_t key) {
    uint64_t hash = GetHash(it, key);
    uint64_t p[2];
    p[0] = hash;
    p[1] = (hash + 1) % it->hash_length;
    bool found = false;
    for (int i = 0; i < 2; i++) {
        for (int j = 0; j < HOP_H / 2; j++) {
            //                printf("local key %d ,%d\n",
            //                get_headernod(p[i])->keys[j],get_headernod(p[i])->valid[j]);
            if (get_headernod(it, p[i])->keys[j] == key) {
                found = true;
                break;
            }
        }
    }
    if (found)
        return 1;
    uint64_t iter = get_headernod(it, hash)->next;
    int count = 0;
    while (iter != 0) {
        count++;
        for (int i = 0; i < 2; i++) {
            if (get_chainnod(it, iter)->keys[i] == key) {
                return 1 + count;
            }
        }
        iter = get_chainnod(it, iter)->next;
    }
    printf("fail to read %d,count %d\n", key, count);
    //    assert(false);
    return NULL;
}

void *Delete(RdmaHopHash *it, uint64_t key) {
    // TODO
    return NULL;
}

#endif
