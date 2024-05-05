#ifndef SSMANAGE_H
#define SSMANAGE_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include "util/spinlock.h"
#include <pthread.h>

// number of nanoseconds in 1 second (1e9)
#define ONE_SECOND_NS 1000000000

//20ms
#define UPDATEPOCH  ONE_SECOND_NS / 1000 * 20

#define PROFILESS

typedef struct SSManage {
    uint64_t thr_num;
    volatile uint64_t readSS;
    volatile int workerNum;
    SpinLock sslock;
    volatile uint64_t *localSS;
    pthread_t update_tid;
    pthread_rwlock_t *rwLock;
    volatile uint64_t curSS;
    void *rawtable; // Assuming RAWTables is a type that needs to be handled similarly in C
#ifdef PROFILESS
    int totalss;
    long totalepoch;
    long maxepoch;
    long minepoch;
#endif
} SSManage;

SSManage* SSManage_create(int thr);
void SSManage_destroy(SSManage* ss);

void* UpdateThread(void *arg);
void RegisterThread(SSManage* ss, int tid);
void UpdateSS(SSManage* ss);
void UpdateReadSS(SSManage* ss);
uint64_t GetLocalSS(SSManage* ss);
uint64_t GetMySS(SSManage* ss);
uint64_t GetReadSS(SSManage* ss);
void WaitAll(SSManage* ss, uint64_t sn);
void UpdateLocalSS(SSManage* ss, uint64_t ss);
int diff_timespec(struct timespec end, struct timespec start);
void ReportProfile(SSManage* ss);

#endif


