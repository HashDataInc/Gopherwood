/********************************************************************
 * 2017 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "core/SharedMemoryContext.h"
#include "common/Configuration.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Logger.h"

namespace Gopherwood {
namespace Internal {

SharedMemoryContext::SharedMemoryContext(std::string dir, shared_ptr<mapped_region> region, int lockFD, bool reset) :
        workDir(dir), mShareMem(region), mLockFD(lockFD) {
    void *addr = region->get_address();
    header = static_cast<ShareMemHeader *>(addr);
    buckets = static_cast<ShareMemBucket *>((void *) ((char *) addr + sizeof(ShareMemHeader)));
    activeStatus = static_cast<ShareMemActiveStatus *>((void *) ((char *) addr + sizeof(ShareMemHeader) +
                sizeof(ShareMemBucket)*header->numBuckets));

    /* Init Shared Memory */
    if (reset) {
        header->reset(Configuration::NUMBER_OF_BLOCKS, Configuration::MAX_CONNECTION);
        memset((char *) addr + sizeof(ShareMemHeader), 0,
               Configuration::NUMBER_OF_BLOCKS * sizeof(ShareMemBucket) +
               Configuration::MAX_CONNECTION * sizeof(ShareMemActiveStatus));
        for (int i=0; i<header->numBuckets; i++) {
            buckets[i].reset();
        }
        for (int i=0; i<header->numMaxActiveStatus; i++) {
            activeStatus[i].reset();
        }
    }
    printStatistics();
}

void SharedMemoryContext::reset() {
    std::memset(mShareMem->get_address(), 0, mShareMem->get_size());
}

void SharedMemoryContext::lock() {
    lockf(mLockFD, F_LOCK, 0);
    header->enter();
}

void SharedMemoryContext::unlock() {
    header->exit();
    lockf(mLockFD, F_ULOCK, 0);
}

int SharedMemoryContext::regist(int pid, FileId fileId) {
    for (int i=0; i<header->numMaxActiveStatus; i++){
        if (activeStatus[i].pid == InvalidPid){
            activeStatus[i].pid = pid;
            activeStatus[i].fileId = fileId;
            activeStatus[i].fileBlockIndex = InvalidBlockId;
            return i;
        }
    }
    return -1;
}

int SharedMemoryContext::unregist(int activeId, int pid){
    if (activeId < 1 || activeId > header->numMaxActiveStatus){
        return -1;
    }
    if (activeStatus[activeId].pid == pid){
        activeStatus[activeId].reset();
        return 0;
    }
    return -1;
}

std::vector<int32_t> SharedMemoryContext::acquireFreeBlock(int activeId, int num) {
    std::vector<int32_t> res;
    int count = num;

    /* pick up from free buckets */
    for (int32_t i = 0; i < header->numBuckets; i++) {
        if (buckets[i].isFreeBucket()) {
            buckets[i].writeOrEvictActiveId = activeId;
            buckets[i].setBucketActive();
            res.push_back(i);
            /* update statistics */
            count--;
            header->numFreeBuckets--;
            header->numActiveBuckets++;

            if (count == 0) {
                break;
            }
        }
    }

    if (count != 0) {
        THROW(GopherwoodSharedMemException,
              "[SharedMemoryContext::acquireBlock] did not acquire enough buckets %d",
              count);
    }

    LOG(INFO, "[SharedMemoryContext::acquireBlock] acquired %lu blocks.", res.size());
    printStatistics();
    return res;
}

std::vector<int32_t> SharedMemoryContext::markEvicting(int activeId, int num){
    std::vector<int32_t> res;
    int count = num;

    if (num <= 0){
        return res;
    }

    /* pick up from used buckets */
    for (int32_t i = 0; i < header->numBuckets; i++) {
        if (buckets[i].isUsedBucket() && !buckets[i].isEvictingBucket()) {
            buckets[i].setBucketEvicting();
            buckets[i].writeOrEvictActiveId = activeId;
            res.push_back(i);
            /* update statistics */
            count--;
            header->numUsedBuckets--;
            header->numEvictingBuckets++;

            if (count == 0) {
                break;
            }
        }
    }
    LOG(INFO, "[SharedMemoryContext::markEvicting] evicting %lu blocks.", res.size());
    printStatistics();
    return res;
}

ShareMemBucket* SharedMemoryContext::evictBlockStart(int32_t bucketId, int activeId){
    assert(bucketId >0 && bucketId<header->numBuckets);
    assert(buckets[bucketId].isEvictingBucket());
    assert(buckets[bucketId].writeOrEvictActiveId == activeId);

    /* mark activestatus evict info  */
    activeStatus[activeId].fileBlockIndex = buckets[bucketId].fileBlockIndex;
    activeStatus[activeId].setEvicting();

    return &buckets[bucketId];
}

/* NOTES: when calling this function, the bucket should be add to preAllocateArray in time! */
void SharedMemoryContext::evictBlockFinish(int32_t bucketId, int activeId) {
    /* reset activestatus evict info  */
    activeStatus[activeId].fileBlockIndex = InvalidBlockId;
    activeStatus[activeId].unsetEvicting();

    /* clear bucket info */
    buckets[bucketId].reset();
    buckets[bucketId].writeOrEvictActiveId = activeId;
    buckets[bucketId].setBucketActive();
    header->numActiveBuckets++;
    header->numEvictingBuckets--;

    LOG(INFO, "[SharedMemoryContext::evictBlockFinish] Bucket %d evict finished.", bucketId);
    printStatistics();
}

/* Transit Bucket State from 1 to 0 */
/* TODO: Remove activestatus mark, if all removed then release this bucket */
void SharedMemoryContext::releaseBlocks(std::vector<Block> &blocks) {
    for (uint32_t i=0; i<blocks.size(); i++) {
        int32_t bucketId = blocks[i].bucketId;
        if (buckets[bucketId].isActiveBucket()){
            buckets[bucketId].reset();
            buckets[bucketId].setBucketFree();
            /* update statistics */
            header->numActiveBuckets--;
            header->numFreeBuckets++;
        } else{
            THROW(
                    GopherwoodSharedMemException,
                    "[SharedMemoryContext::releaseBlock] state of bucket %d is not Active",
                    bucketId);
        }
    }
    LOG(INFO, "[SharedMemoryContext::releaseBlocks] released %lu blocks.", blocks.size());
    printStatistics();
}

/* Transit Bucket State from 1 to 2 */
/* TODO: Remove activestatus mark, if all removed then inactivate this bucket */
void SharedMemoryContext::inactivateBlocks(std::vector<Block> &blocks, FileId fileId){
    for (uint32_t i=0; i<blocks.size(); i++) {
        Block b = blocks[i];
        if (buckets[b.bucketId].isActiveBucket()){
            buckets[b.bucketId].fileId = fileId;
            buckets[b.bucketId].fileBlockIndex = b.blockId;
            buckets[b.bucketId].writeOrEvictActiveId = InvalidActiveId;
            /* TODO: reset all read activeId to 0 */
            buckets[b.bucketId].setBucketFree();
            /* update statistics */
            header->numActiveBuckets--;
            header->numUsedBuckets++;
        } else{
            THROW(GopherwoodSharedMemException,
                  "[SharedMemoryContext::releaseBlock] state of bucket %d is not Active",
                  b.bucketId);
        }
    }
    LOG(INFO, "[SharedMemoryContext::inactivateBlocks] inactivated %lu blocks.", blocks.size());
    printStatistics();
}

int SharedMemoryContext::calcDynamicQuotaNum() {
    return Configuration::CUR_QUOTA_SIZE;
}


std::string &SharedMemoryContext::getWorkDir() {
    return workDir;
}

int32_t SharedMemoryContext::getNumMaxActiveStatus(){
    return header->numMaxActiveStatus;
}

int32_t SharedMemoryContext::getFreeBucketNum(){
    return header->numFreeBuckets;
}

int32_t SharedMemoryContext::getActiveBucketNum(){
    return header->numActiveBuckets;
}

int32_t SharedMemoryContext::getUsedBucketNum(){
    return header->numUsedBuckets;
}

int32_t SharedMemoryContext::getEvictingBucketNum(){
    return header->numEvictingBuckets;
}

void SharedMemoryContext::printStatistics() {
    LOG(INFO, "[SharedMemoryContext::printStatistics] free %d, active %d, used %d, evicting %d",
        header->numFreeBuckets, header->numActiveBuckets, header->numUsedBuckets, header->numEvictingBuckets);
}

SharedMemoryContext::~SharedMemoryContext() {

}

}
}
