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
#include <common/Configuration.h>
#include "SharedMemoryContext.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Logger.h"

namespace Gopherwood {
namespace Internal {

SharedMemoryContext::SharedMemoryContext(std::string dir, shared_ptr<mapped_region> region, int lockFD) :
        workDir(dir), mShareMem(region), mLockFD(lockFD) {
    void *addr = region->get_address();
    header = static_cast<ShareMemHeader *>(addr);
    buckets = static_cast<ShareMemBucket *>((void *) ((char *) addr + sizeof(ShareMemHeader)));
    conns = static_cast<ShareMemConn *>((void *) ((char *) addr + sizeof(ShareMemHeader) +
                sizeof(ShareMemBucket)*header->numBuckets));
}

int SharedMemoryContext::regist(int pid) {
    for (int i=0; i<header->numMaxConn; i++){
        if (conns[i].pid == 0){
            conns[i].pid = pid;
            return i;
        }
    }
    return -1;
}

int SharedMemoryContext::unregist(int connId, int pid){
    if (connId < 0 || connId > header->numMaxConn){
        return -1;
    }
    if (conns[connId].pid == pid){
        conns[connId].pid = 0;
        return 0;
    }
    return -1;
}

void SharedMemoryContext::reset() {
    std::memset(mShareMem->get_address(), 0, mShareMem->get_size());
}

std::vector<int32_t> SharedMemoryContext::acquireBlock(FileId fileId, int num) {
    std::vector<int32_t> res;
    int count = num;

    /* pick up from free buckets */
    for (int32_t i = 0; i < header->numBuckets; i++) {
        if (buckets[i].isFreeBucket()) {
            LOG(INFO, "[SharedMemoryContext::acquireBlock] got one free bucket(%d)", i);
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

    /* TODO: pick up from Used buckets and evict them */
    if (count > 0) {
    }

    if (count != 0) {
        THROW(GopherwoodSharedMemException,
              "[SharedMemoryContext::acquireBlock] did not acquire enough buckets %d",
              count);
    }

    LOG(INFO, "[SharedMemoryContext::acquireBlock] acquired %d blocks.", count);
    printStatistics();
    return res;
}

void SharedMemoryContext::releaseBlocks(std::vector<Block> &blocks) {
    for (uint32_t i=0; i<blocks.size(); i++) {
        int32_t bucketId = blocks[i].bucketId;
        if (buckets[bucketId].isActiveBucket()){
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

void SharedMemoryContext::inactivateBlocks(std::vector<Block> &blocks, FileId fileId){
    for (uint32_t i=0; i<blocks.size(); i++) {
        Block b = blocks[i];
        if (buckets[b.bucketId].isActiveBucket()){
            buckets[b.bucketId].fileId = fileId;
            buckets[b.bucketId].fileBlockIndex = b.blockId;
            buckets[b.bucketId].setBucketFree();
            /* update statistics */
            header->numActiveBuckets--;
            header->numUsedBuckets++;
        } else{
            THROW(
                    GopherwoodSharedMemException,
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

void SharedMemoryContext::lock() {
    lockf(mLockFD, F_LOCK, 0);
    header->enter();
}

void SharedMemoryContext::unlock() {
    header->exit();
    lockf(mLockFD, F_ULOCK, 0);
}

std::string &SharedMemoryContext::getWorkDir() {
    return workDir;
}

int32_t SharedMemoryContext::getNumMaxConn(){
    return header->numMaxConn;
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

void SharedMemoryContext::printStatistics() {
    LOG(INFO, "[SharedMemoryContext::printStatistics] free %d, active %d, used %d",
        header->numFreeBuckets, header->numActiveBuckets, header->numUsedBuckets);
}

SharedMemoryContext::~SharedMemoryContext() {

}

}
}
