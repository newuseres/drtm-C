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

/*
 *  Cuckoo hash table using RDMA  - JiaXin
 */

#ifndef RDMACUCKOOHASH_H
#define RDMACUCKOOHASH_H
typedef unsigned long long uint64_t;

#include <stdlib.h>
#include <iostream>
#include "c_std/vector/Vector.h"

// paddings for RDMA,may not be needed

#define SLOT_PER_BUCKET 4
#define MAX_TRY 500

uint64_t MurmurHash64A(uint64_t key, unsigned int seed)
{

  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;
  uint64_t h = seed ^ (8 * m);
  const uint64_t *data = &key;
  const uint64_t *end = data + 1;

  while (data != end)
  {
    uint64_t k = *data++;
    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
  }

  const unsigned char *data2 = (const unsigned char *)data;

  switch (8 & 7)
  {
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

struct RdmaArrayNode
{
  uint64_t key;
  uint64_t index;
  bool valid;
};

struct RdmaCuckooHash
{
  char *array;
  int length;
  int entrysize;
  int bucketlength;
  int bucketsize;
  int size; // total
  int data_offset;
  int free_ptr;
  RdmaArrayNode *header;
};

RdmaCuckooHash *RdmaCuckooHash_new(int esize, int len, char *arr)
{
  RdmaCuckooHash *it = (RdmaCuckooHash *)malloc(sizeof(RdmaCuckooHash));
  it->entrysize = (((esize - 1) >> 3) + 1) << 3;
  ;
  it->bucketsize = sizeof(RdmaArrayNode) * SLOT_PER_BUCKET;
  it->length = len;
  it->bucketlength = it->length / SLOT_PER_BUCKET;
  it->array = arr;
  it->data_offset = it->bucketlength * it->bucketsize;
  it->free_ptr = 0;
  it->size = it->data_offset + it->entrysize * it->length;

  it->header = (RdmaArrayNode *)it->array;
  return it;
}

void RdmaCuckooHash_free(RdmaCuckooHash *it)
{
}

uint64_t GetHash(RdmaCuckooHash *it, uint64_t key)
{
  return MurmurHash64A(key, 0xdeadbeef) % it->bucketlength;
}

uint64_t GetHash2(RdmaCuckooHash *it, uint64_t key)
{
  return key % it->bucketlength;
}
uint64_t get_dataloc(RdmaCuckooHash *it, int index)
{
  return it->data_offset + it->entrysize * index;
}
// void find_path(RdmaCuckooHash *it, uint64_t start_pos, vector<uint64_t> &pos_vec)
void find_path(RdmaCuckooHash *it, uint64_t start_pos, Vector *pos_vec)
{
  int depth = 0;
  uint64_t kick_pos = start_pos;
  while (depth < MAX_TRY)
  {
    RdmaArrayNode *node = &it->header[kick_pos];
    uint64_t kick_key = node->key;
    // pos_vec.push_back(kick_pos);
    vector_push_back(pos_vec, &kick_pos);
    uint64_t another_hash = (GetHash(it, kick_key) == (kick_pos / SLOT_PER_BUCKET) ? GetHash2(it, kick_key) : GetHash(it, kick_key));
    depth++;
    for (int i = 0; i < SLOT_PER_BUCKET; i++)
    {
      node = &it->header[another_hash * SLOT_PER_BUCKET + i];
      if (node->valid == false)
      {
        // find a empty slot
        // pos_vec.push_back(another_hash * SLOT_PER_BUCKET + i);
        uint64_t tmp = another_hash * SLOT_PER_BUCKET + i;
        vector_push_back(pos_vec, &tmp);
        return;
      }
    }
    kick_pos = another_hash * SLOT_PER_BUCKET + rand() % SLOT_PER_BUCKET;
  }
  //  assert(false);
}
void Insert(RdmaCuckooHash *it, uint64_t key, void *val)
{
  uint64_t p[2];
  p[0] = GetHash(it, key);
  p[1] = GetHash2(it, key);
  for (int slot = 0; slot < 2; slot++)
  {
    for (int i = 0; i < SLOT_PER_BUCKET; i++)
    {
      RdmaArrayNode *node = &it->header[p[slot] * SLOT_PER_BUCKET + i];
      if (node->valid == false)
      {
        node->valid = true;
        node->key = key;
        node->index = it->free_ptr;
        memcpy((void *)(it->array + get_dataloc(it, it->free_ptr)), val, it->entrysize);
        it->free_ptr++;
        return;
      }
    }
  }

  //// didn't find empty slot at first
  uint64_t kick_pos = (rand() % 2 == 0 ? p[0] : p[1]) * SLOT_PER_BUCKET + rand() % SLOT_PER_BUCKET;
  //  vector<uint64_t> pos_vec; ljh change
  Vector *pos_vec = vector_create(sizeof(uint64_t));
  find_path(it, kick_pos, pos_vec);
  int pointer = vector_size(pos_vec) - 1;
  // RdmaArrayNode *node = &it->header[pos_vec[pointer]];
  RdmaArrayNode *node = &it->header[*(uint64_t *)vector_at(pos_vec, pointer)];
  node->valid = true;
  while (pointer > 0)
  {
    //    RdmaArrayNode *prev = &it->header[pos_vec[pointer - 1]];
    RdmaArrayNode *prev = &it->header[*(uint64_t *)vector_at(pos_vec, pointer - 1)];
    node->key = prev->key;
    node->index = prev->index;
    //            memcpy((void*)(node+1),(void*)(prev+1),entrysize);
    node = prev;
    pointer--;
  }
  node->key = key;
  node->index = it->free_ptr;
  memcpy((void *)(it->array + get_dataloc(it, it->free_ptr)), val, it->entrysize);
  it->free_ptr++;

  // ljh change free vector
  vector_deallocate(pos_vec);
  return;
}
uint64_t *Get(RdmaCuckooHash *it, uint64_t key)
{
  uint64_t p[2];
  p[0] = GetHash(it, key);
  p[1] = GetHash2(it, key);
  for (int slot = 0; slot < 2; slot++)
  {
    for (int i = 0; i < SLOT_PER_BUCKET; i++)
    {
      char *bucket_addr = it->array + p[slot] * it->bucketsize;
      RdmaArrayNode *node = &it->header[p[slot] * SLOT_PER_BUCKET + i];
      if (node->valid == true && node->key == key)
      {
        return (uint64_t *)(it->array + get_dataloc(it, node->index));
      }
    }
  }
  return NULL;
}

void *Delete(RdmaCuckooHash *it, uint64_t key)
{
  // TODO
  return NULL;
}

#endif
