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
                            Configuration::NUMBER_OF_BLOCKS*sizeof(ShareMemBucket)));

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
            LOG(INFO, "[SharedMemoryContext] activeId %d, pid=%d", i, activeStatus[i].pid);
            return i;
        }
    }
    return -1;
}

int SharedMemoryContext::unregist(int activeId, int pid){
    LOG(INFO, "[SharedMemoryContext] Start activeId = %d, pid = %d", activeId, pid);
    if (activeId < 0 || activeId >= header->numMaxActiveStatus){
        return -1;
    }
    LOG(INFO, "[SharedMemoryContext] activeStatus[activeId].pid=%d", activeStatus[activeId].pid);
    if (activeStatus[activeId].pid == pid){
        activeStatus[activeId].reset();
        return 0;
    }
    return -1;
}

/* Check if all ActiveStatus of a given FileId are unregisted.
 * This is called by ActiveStatus close, manifest log will be truncated if
 * this function returns true */
bool SharedMemoryContext::isLastActiveStatusOfFile(FileId fileId){
    for (int i=0; i<Configuration::MAX_CONNECTION; i ++){
        if (activeStatus[i].fileId == fileId){
            return false;
        }
    }
    LOG(INFO, "[SharedMemoryContext] Is last active status of file %lu-%u",
    fileId.hashcode, fileId.collisionId);
    return true;
}

/* Activate a number of Free Buckets (0->1)*/
std::vector<int32_t> SharedMemoryContext::acquireFreeBucket(int activeId, int num, FileId fileId, bool isWrite) {
    std::vector<int32_t> res;
    int count = num;

    /* pick up from free buckets */
    for (int32_t i = 0; i < header->numBuckets; i++) {
        if (buckets[i].isFreeBucket()) {
            buckets[i].setBucketActive();
            buckets[i].fileId = fileId;
            if (isWrite){
                buckets[i].markWrite(activeId);
            }else {
                buckets[i].markRead(activeId);
            }
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

    LOG(INFO, "[SharedMemoryContext] acquired %lu blocks.", res.size());
    printStatistics();
    return res;
}

/* When activate buckets from Used Buckets(2->1), we need to evict the data first.
 * The steps are:
 * 1. markEvicting -- make sure no other ActiveStatus can evict same bucket
 * 2. evictBlockStart -- set the evicting ActiveStatus status in SharedMem.
 *             This will mark write/read blockId to inform other activestatus
 *             of same File
 * 3. evictBlockFinish -- finally reset evicting ActiveStatus and activate the bucket */
std::vector<int32_t> SharedMemoryContext::markBucketEvicting(int activeId, int num){
    std::vector<int32_t> res;
    int count = num;

    if (num <= 0){
        return res;
    }

    /* pick up from used buckets */
    for (int32_t i = 0; i < header->numBuckets; i++) {
        if (buckets[i].isUsedBucket() && !buckets[i].isEvictingBucket()) {
            buckets[i].setBucketEvicting();
            buckets[i].evictActiveId = activeId;
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
    LOG(INFO, "[SharedMemoryContext] evicting %lu blocks.", res.size());
    printStatistics();
    return res;
}

BlockInfo SharedMemoryContext::evictBucketStart(int32_t bucketId, int activeId){
    assert(bucketId >0 && bucketId<header->numBuckets);
    assert(buckets[bucketId].isEvictingBucket());
    assert(buckets[bucketId].evictActiveId == activeId);
    BlockInfo info;

    /* mark activestatus evict info  */
    activeStatus[activeId].fileBlockIndex = buckets[bucketId].fileBlockIndex;
    activeStatus[activeId].setEvicting();

    info.fileId = buckets[bucketId].fileId;
    info.blockId = buckets[bucketId].fileBlockIndex;
    info.isLocal = true;
    info.offset = InvalidBlockOffset;

    return info;
}

void SharedMemoryContext::evictBucketFinish(int32_t bucketId, int activeId, FileId fileId, int isWrite) {
    /* reset activestatus evict info  */
    activeStatus[activeId].fileBlockIndex = InvalidBlockId;
    activeStatus[activeId].unsetEvicting();

    /* clear bucket info */
    buckets[bucketId].reset();
    buckets[bucketId].setBucketActive();
    buckets[bucketId].fileId = fileId;
    if (isWrite){
        buckets[bucketId].markWrite(activeId);
    }else{
        buckets[bucketId].markRead(activeId);
    }

    header->numActiveBuckets++;
    header->numEvictingBuckets--;

    LOG(INFO, "[SharedMemoryContext] Bucket %d evict finished.", bucketId);
    printStatistics();
}

/* Transit Bucket State from 1 to 0 */
/* TODO: Remove activestatus mark, if all removed then release this bucket */
void SharedMemoryContext::releaseBuckets(std::vector<Block> &blocks) {
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
    LOG(INFO, "[SharedMemoryContext] released %lu blocks.", blocks.size());
    printStatistics();
}

/* Activate a block from status 2 to 1. No need to evict because it's just the same File */
bool SharedMemoryContext::activateBucket(FileId fileId, Block& block, int activeId, bool isWrite) {
    int32_t bucketId = block.bucketId;
    if (buckets[bucketId].fileId.hashcode != fileId.hashcode ||
        buckets[bucketId].fileId.collisionId != fileId.collisionId ||
        buckets[bucketId].fileBlockIndex != block.blockId){
        THROW(GopherwoodSharedMemException,
              "[SharedMemoryContext::activateBlock] File Id mismatch, expect fileId = %lu-%u, "
                      "current bucket fileId=%lu-%u",
              fileId.hashcode, fileId.collisionId,
              buckets[bucketId].fileId.hashcode, buckets[bucketId].fileId.collisionId
        );
    }

    if (buckets[bucketId].isUsedBucket()) {
        buckets[bucketId].setBucketActive();
        LOG(INFO, "[SharedMemoryContext] bucket %d activated. state %d", bucketId, buckets[bucketId].flags);
        if (isWrite) {
            buckets[bucketId].markWrite(activeId);
            LOG(INFO, "[SharedMemoryContext] write activeId %d, bucketId %d", activeId, bucketId);
        }else{
            buckets[bucketId].markRead(activeId);
            LOG(INFO, "[SharedMemoryContext] read activeId %d, bucketId %d", activeId, bucketId);
        }
        header->numUsedBuckets--;
        header->numActiveBuckets++;
        return true;
    } else if(buckets[bucketId].isActiveBucket()){
        if (isWrite) {
            buckets[bucketId].markWrite(activeId);
        }else{
            buckets[bucketId].markRead(activeId);
        }
        return false;
    } else if(buckets[bucketId].isEvictingBucket()){
        THROW(GopherwoodNotImplException,
              "[SharedMemoryContext::activateBlock] activate an evicting bucket is not implemented yet");
        return false;
    } else{
        THROW(GopherwoodNotImplException,
              "[SharedMemoryContext::activateBlock]  not implemented yet");
        return false;
    }

}

/* Transit Bucket State from 1 to 2 */
std::vector<Block> SharedMemoryContext::inactivateBuckets(std::vector<Block> &blocks, FileId fileId, int activeId, bool isWrite){
    std::vector<Block> res;
    for (uint32_t i=0; i<blocks.size(); i++) {
        Block b = blocks[i];
        if (buckets[b.bucketId].isActiveBucket()){
            buckets[b.bucketId].fileId = fileId;
            buckets[b.bucketId].fileBlockIndex = b.blockId;
            if (isWrite){
                buckets[b.bucketId].unmarkWrite(activeId);
            } else{
                buckets[b.bucketId].unmarkRead(activeId);
            }

            /* only return 1->0 blocks for manifest logging */
            if (buckets[b.bucketId].noActiveReadWrite()){
                buckets[b.bucketId].setBucketUsed();
                res.push_back(b);
                /* update statistics */
                header->numActiveBuckets--;
                header->numUsedBuckets++;
            }
        } else{
            THROW(GopherwoodSharedMemException,
                  "[SharedMemoryContext] state of bucket %d is not Active when trying to release block",
                  b.bucketId);
        }
    }
    LOG(INFO, "[SharedMemoryContext] inactivated %lu blocks.", blocks.size());
    printStatistics();
    return res;
}

/* Transit Bucket State from 1 to 0 */
std::vector<Block> SharedMemoryContext::deleteBuckets(std::vector<Block> &blocks){
    std::vector<Block> res;
    for (uint32_t i=0; i<blocks.size(); i++) {
        int32_t bucketId = blocks[i].bucketId;
        if (buckets[bucketId].isUsedBucket()){
            buckets[bucketId].reset();
            buckets[bucketId].setBucketFree();
            /* update statistics */
            header->numUsedBuckets--;
            header->numFreeBuckets++;
        } else{
            THROW(
                    GopherwoodSharedMemException,
                    "[SharedMemoryContext::deleteBuckets] state of bucket %d is not Used",
                    bucketId);
        }
    }
    LOG(INFO, "[SharedMemoryContext] deleteBuckets %lu blocks.", blocks.size());
    printStatistics();
    return res;
}

void SharedMemoryContext::updateActiveFileInfo(std::vector<Block> &blocks, FileId fileId){
    for (uint32_t i=0; i<blocks.size(); i++) {
        Block b = blocks[i];
        if (buckets[b.bucketId].fileId == fileId) {
            buckets[b.bucketId].fileBlockIndex = b.blockId;
        }else{
            THROW(GopherwoodSharedMemException,
                  "[SharedMemoryContext] FileId mismatch, expectging %s, actually %s",
                  fileId.toString().c_str(), buckets[b.bucketId].fileId.toString().c_str());
        }
    }
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
    LOG(INFO, "[SharedMemoryContext] free %d, active %d, used %d, evicting %d",
        header->numFreeBuckets, header->numActiveBuckets, header->numUsedBuckets, header->numEvictingBuckets);
}

SharedMemoryContext::~SharedMemoryContext() {

}

}
}
