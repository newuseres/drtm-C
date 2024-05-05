#include "snapshotmanage.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>

#include "memstore/rawtables.h"

__thread int tid_;

typedef struct SSManage {
    int thr_num;
    uint64_t* localSS;
    uint64_t curSS;
    uint64_t readSS;
    int workerNum;
    pthread_t update_tid;
#ifdef PROFILESS
    long totalss;
    long totalepoch;
    long maxepoch;
    long minepoch;
#endif
} SSManage;

SSManage* SSManage_new(int thr) {
    SSManage* obj = (SSManage*)malloc(sizeof(SSManage));
    obj->thr_num = thr;
    obj->localSS = (uint64_t*)malloc(sizeof(uint64_t) * thr);
    obj->curSS = 1;
    obj->readSS = 0;
    obj->workerNum = 0;
#ifdef PROFILESS
    obj->totalss = 0;
    obj->totalepoch = 0;
    obj->maxepoch = 0;
    obj->minepoch = 0;
#endif
    pthread_create(&(obj->update_tid), NULL, UpdateThread, (void *)obj);
    return obj;
}

void SSManage_delete(SSManage* obj) {
    free(obj->localSS);
    free(obj);
}

int diff_timespec(struct timespec end, struct timespec start) {
    int diff = (end.tv_sec > start.tv_sec) ? (end.tv_sec - start.tv_sec) * 1000 : 0;
    assert(diff || end.tv_sec == start.tv_sec);
    if (end.tv_nsec > start.tv_nsec) {
        diff += (end.tv_nsec - start.tv_nsec) / 1000000;
    } else {
        diff -= (start.tv_nsec - end.tv_nsec) / 1000000;
    }
    return diff;
}

void* UpdateThread(void* arg) {
    SSManage* ssmm = (SSManage*)arg;
    while (1) {
#ifdef PROFILESS
        struct timespec start, end;
        clock_gettime(CLOCK_REALTIME, &start);
#endif
        struct timespec t;
        t.tv_sec = UPDATEPOCH / ONE_SECOND_NS;
        t.tv_nsec = UPDATEPOCH % ONE_SECOND_NS;
        nanosleep(&t, NULL);
        ssmm->curSS++;
#ifdef PROFILESS
        clock_gettime(CLOCK_REALTIME, &end);
        int epoch = diff_timespec(end, start);
        ssmm->totalepoch += epoch;
        ssmm->totalss++;
        if (ssmm->maxepoch < epoch)
            ssmm->maxepoch = epoch;
        if (ssmm->minepoch > epoch || ssmm->minepoch == 0)
            ssmm->minepoch = epoch;
#endif
    }
    return NULL;
}

void SSManage_ReportProfile(SSManage* ssmm) {
    printf("Avg Epoch %ld (ms) Max Epoch %ld (ms) Min Epoch %ld (ms) Snap Update Number %d\n",
           ssmm->totalepoch / ssmm->totalss, ssmm->maxepoch, ssmm->minepoch, ssmm->totalss);
}

void SSManage_RegisterThread(SSManage* ssmm, int tid) {
    tid_ = tid;
    ssmm->localSS[tid] = 1;
}

uint64_t SSManage_GetLocalSS(SSManage* ssmm) {
    assert(tid_ < ssmm->thr_num);
    return ssmm->curSS;
}

uint64_t SSManage_GetMySS(SSManage* ssmm) {
    return ssmm->localSS[tid_];
}

uint64_t SSManage_GetReadSS(SSManage* ssmm) {
    ssmm->readSS = ssmm->localSS[0];
    for (int i = 1; i < ssmm->thr_num; i++)
        if (ssmm->readSS > ssmm->localSS[i]) ssmm->readSS = ssmm->localSS[i];
    ssmm->readSS--;
    return ssmm->readSS;
}

void SSManage_UpdateLocalSS(SSManage* ssmm, uint64_t ss) {
    ssmm->localSS[tid_] = ss;
}

void SSManage_WaitAll(SSManage* ssmm, uint64_t ss) {
    for (int i = 0; i < ssmm->thr_num; i++) {
        while (ssmm->localSS[i] < ss);
    }
}


