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
#include "SharedMemoryContext.h"
#include "common/Logger.h"

namespace Gopherwood {
namespace Internal {

SharedMemoryContext::SharedMemoryContext(std::string dir, shared_ptr<mapped_region> region, int lockFD) :
        workDir(dir), mShareMem(region), mLockFD(lockFD) {
    void *addr = region->get_address();
    header = static_cast<ShareMemHeader *>(addr);
    buckets = static_cast<ShareMemBucket *>((void *) ((char *) addr + sizeof(ShareMemHeader)));
}

void SharedMemoryContext::reset() {
    std::memset(mShareMem->get_address(), 0, mShareMem->get_size());
}

std::vector<int32_t> SharedMemoryContext::acquireBlock(FileId fileId) {
    std::vector<int32_t> res;

    int numBlocksToAcquire = calcBlockAcquireNum();
    LOG(INFO, "[SharedMemoryContext::acquireBlock] need to acquire %d blocks.", numBlocksToAcquire);
    /* pick up from free buckets */
    for (int32_t i = 0; i < header->numBlocks; i++) {
        if (buckets[i].isFreeBucket()) {
            LOG(INFO, "[SharedMemoryContext::acquireBlock] got one free bucket(%d)", i);
            buckets[i].setBucketActive();
            res.push_back(i);
            numBlocksToAcquire--;
            if (numBlocksToAcquire == 0) {
                break;
            }
        }
    }

    /* TODO: pick up from Used buckets and evict them */
    if (numBlocksToAcquire > 0) {
    }

    return res;
}

int SharedMemoryContext::calcBlockAcquireNum() {
    return 1;
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

SharedMemoryContext::~SharedMemoryContext() {

}

}
}
