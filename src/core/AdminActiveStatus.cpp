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

#include "core/AdminActiveStatus.h"
#include "common/Configuration.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Logger.h"
#include "core/Manifest.h"

namespace Gopherwood {
namespace Internal {


#define SHARED_MEM_BEGIN    try { \
                                mSharedMemoryContext->lock();

#define SHARED_MEM_END          mSharedMemoryContext->unlock();\
                            } catch (...) { \
                                SetLastException(Gopherwood::current_exception()); \
                                mSharedMemoryContext->unlock(); \
                                Gopherwood::rethrow_exception(Gopherwood::current_exception()); \
                            }

AdminActiveStatus::AdminActiveStatus(shared_ptr<SharedMemoryContext> sharedMemoryContext,
                                     int localSpaceFD) :
        BaseActiveStatus(sharedMemoryContext, localSpaceFD) {
    SHARED_MEM_BEGIN
        registInSharedMem();
    SHARED_MEM_END
}

void AdminActiveStatus::registInSharedMem() {
    mActiveId = mSharedMemoryContext->registAdmin(getpid());
    if (mActiveId == -1) {
        THROW(GopherwoodSharedMemException,
              "[ActiveStatus::registInSharedMem] Exceed max connection limitation %d",
              mSharedMemoryContext->getNumMaxActiveStatus());
    }
    LOG(DEBUG1, "[ActiveStatus]          |"
            "Registered successfully, ActiveID=%d, PID=%d", mActiveId, getpid());
}

void AdminActiveStatus::unregistInSharedMem() {
    if (mActiveId == -1)
        return;

    int rc = mSharedMemoryContext->unregistAdmin(mActiveId, getpid());
    if (rc != 0) {
        mSharedMemoryContext->unlock();
        THROW(GopherwoodSharedMemException,
              "[ActiveStatus::unregistInSharedMem] connection info mismatch with SharedMem ActiveId=%d, PID=%d",
              mActiveId, getpid());
    }

    LOG(DEBUG1, "[ActiveStatus]          |"
            "Unregistered successfully, ActiveID=%d, PID=%d", mActiveId, getpid());
    mActiveId = -1;
}

void AdminActiveStatus::getShareMemStatistic(GWSysInfo* sysInfo) {
    SHARED_MEM_BEGIN
        sysInfo->numActiveBuckets = mSharedMemoryContext->getActiveBucketNum();
        sysInfo->numEvictingBuckets = mSharedMemoryContext->getEvictingBucketNum();
        sysInfo->numUsedBuckets = mSharedMemoryContext->getUsedBucketNum();
        sysInfo->numFreeBuckets = mSharedMemoryContext->getFreeBucketNum();
        sysInfo->numLoadingBuckets = mSharedMemoryContext->getLoadingBucketNum();
        sysInfo->numAdminActiveStatus = mSharedMemoryContext->getAdminActiveStatusNum();
        sysInfo->numFileActiveStatus = mSharedMemoryContext->getFileActiveStatusNum();
    SHARED_MEM_END
}

int32_t AdminActiveStatus::evictNumOfBlocks(int num) {
    int numToEvict = num;
    int numEvicted = 0;
    BlockInfo   evictBlockInfo;
    bool found = false;

    while (numToEvict > 0) {
        SHARED_MEM_BEGIN
            if (mSharedMemoryContext->getUsedBucketNum() > 0) {
                evictBlockInfo = mSharedMemoryContext->markBucketEvicting(mActiveId);
                found = true;
            }
        SHARED_MEM_END

        if (found) {
            mOssWorker->writeBlock(evictBlockInfo);
        }

        SHARED_MEM_BEGIN
            int rc = mSharedMemoryContext->evictBucketFinishAndTryFree(evictBlockInfo.bucketId, mActiveId);

            if (rc == 0) {
                logEvictBlock(evictBlockInfo);
                mNumEvicted ++;
            } else if (rc == 1 || rc == 2) {
                /* the evicted bucket has been activated by it's file owner, give up this one */
                mOssWorker->deleteBlock(evictBlockInfo);
            }
        SHARED_MEM_END
    }

    return numEvicted;
}


void AdminActiveStatus::logEvictBlock(BlockInfo info) {
    Block block(InvalidBucketId, info.blockId, false, BUCKET_FREE);

        /* check file exist */
        std::string manifestFileName = Manifest::getManifestFileName(mSharedMemoryContext->getWorkDir(), info.fileId);
        if (access(manifestFileName.c_str(), F_OK) == -1) {
            THROW(GopherwoodInvalidParmException,
                  "[ActiveStatus::ActiveStatus] File does not exist %s",
                  manifestFileName.c_str());
        }
        Manifest *manifest = new Manifest(manifestFileName);
        manifest->mfSeek(0, SEEK_END);
        manifest->logEvcitBlock(block);

}

AdminActiveStatus::~AdminActiveStatus() {
    unregistInSharedMem();
}

}
}