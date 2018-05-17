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
                                                                 Configuration::NUMBER_OF_BLOCKS *
                                                                 sizeof(ShareMemBucket)));

    /* Init Shared Memory */
    if (reset) {
        header->reset(Configuration::NUMBER_OF_BLOCKS, Configuration::MAX_CONNECTION);
        memset((char *) addr + sizeof(ShareMemHeader), 0,
               Configuration::NUMBER_OF_BLOCKS * sizeof(ShareMemBucket) +
               Configuration::MAX_CONNECTION * sizeof(ShareMemActiveStatus));
        for (int i = 0; i < header->numBuckets; i++) {
            buckets[i].reset();
        }
        for (int i = 0; i < header->numMaxActiveStatus; i++) {
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

int16_t SharedMemoryContext::regist(int pid, FileId fileId, bool isWrite, bool isDelete) {
    int16_t activeId = -1;
    bool shouldDestroy = isDelete ? true : false;

    for (int i = 0; i < header->numMaxActiveStatus; i++) {
        /* validations */
        if (activeStatus[i].fileId == fileId) {
            /* If the file already marked deleted but still opening by some others,
             * New open action is still allowed, but the newly created ActiveStatus
             * should also mark shouldDestroy. */
            if (activeStatus[i].shouldDestroy()) {
                shouldDestroy = true;
            }
            /* If the current regist requester is for delete, mark shouldDestroy for
             * all existing openings */
            if (isDelete) {
                /* mark should destroy on this activestatus */
                activeStatus[i].setShouldDestroy();
            }
        }

        /* assign new activeId */
        if (activeId == -1 && activeStatus[i].pid == InvalidPid) {
            activeStatus[i].pid = pid;
            activeStatus[i].fileId = fileId;
            activeStatus[i].fileBlockIndex = InvalidBlockId;
            if (isDelete) {
                activeStatus[i].setForDelete();
            }
            activeId = i;
        }
    }
    if (shouldDestroy && activeId != -1) {
        activeStatus[activeId].setShouldDestroy();
    }

    LOG(DEBUG1, "[SharedMemoryContext]   |"
              "Regist file %s, activeId=%d, pid=%d, %s",
        fileId.toString().c_str(),activeId, activeStatus[activeId].pid,
        shouldDestroy ? "should destroy":"no need to destroy");
    return activeId;
}

int SharedMemoryContext::unregist(int16_t activeId, int pid, bool *shouldDestroy) {
    if (activeId < 0 || activeId >= header->numMaxActiveStatus) {
        return -1;
    }

    if (activeStatus[activeId].pid == pid) {
        if (activeStatus[activeId].shouldDestroy()){
            *shouldDestroy = true;
        }
        LOG(DEBUG1, "[SharedMemoryContext]   |"
                "Unregist File %s, activeId=%d, pid=%d, %s",
            activeStatus[activeId].fileId.toString().c_str(),
            activeId, activeStatus[activeId].pid,
            *shouldDestroy ? "should destroy":"no need to destroy");
        activeStatus[activeId].reset();
        return 0;
    }
    return -1;
}

/* Check if all ActiveStatus of a given FileId are unregisted.
 * This is called by ActiveStatus close, manifest log will be truncated if
 * this function returns true */
bool SharedMemoryContext::isFileOpening(FileId fileId) {
    for (uint16_t i = 0; i < Configuration::MAX_CONNECTION; i++) {
        if (activeStatus[i].fileId == fileId) {
            return true;
        }
    }
    LOG(DEBUG1, "[SharedMemoryContext]   |"
              "No more active status of file %s",
        fileId.toString().c_str());
    return false;
}

/* Activate a number of Free Buckets (0->1)*/
std::vector<int32_t> SharedMemoryContext::acquireFreeBucket(int16_t activeId, int num, FileId fileId, bool isWrite) {
    std::vector<int32_t> res;
    int count = num;

    /* pick up from free buckets */
    for (int32_t i = 0; i < header->numBuckets; i++) {
        if (buckets[i].isFreeBucket()) {
            buckets[i].setBucketActive();
            buckets[i].fileId = fileId;
            if (isWrite) {
                buckets[i].markWrite(activeId);
            } else {
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

    LOG(DEBUG1, "[SharedMemoryContext]   |"
              "Acquired %lu free buckets.", res.size());
    printStatistics();
    return res;
}

/* When activate buckets from Used Buckets(2->1), we need to evict the data first.
 * The steps are:
 * 1. markBucketEvicting -- set the evicting ActiveStatus status in SharedMem.
 *             This will mark write/read blockId to inform other activestatus
 *             of same File
 * 2. evictBlockFinish -- finally reset evicting ActiveStatus and activate the bucket */
BlockInfo SharedMemoryContext::markBucketEvicting(int16_t activeId) {
    BlockInfo info;

    /* Run the "clock sweep" algorithm */
    int32_t trycounter = header->numBuckets;
    for (;;)
    {
        int32_t bucketId = header->nextVictimBucket;
        ShareMemBucket* bucket = &buckets[header->nextVictimBucket];

        if (++header->nextVictimBucket >= header->numBuckets) {
            header->nextVictimBucket = 0;
        }

        if (bucket->isUsedBucket() && !bucket->isEvictingBucket())
        {
            if (bucket->usageCount > 0) {
                bucket->usageCount--;
                trycounter = header->numBuckets;
            }
            else
            {
                /* Found a usable buffer */
                bucket->setBucketEvicting();
                bucket->evictLoadActiveId = activeId;

                /* fill ActiveStatus evict info */
                activeStatus[activeId].evictFileId = bucket->fileId;
                activeStatus[activeId].fileBlockIndex = bucket->fileBlockIndex;
                activeStatus[activeId].setEvicting();

                /* fill result BlockInfo */
                info.fileId = bucket->fileId;
                info.blockId = bucket->fileBlockIndex;
                info.bucketId = bucketId;
                info.isLocal = true;
                info.offset = InvalidBlockOffset;
                info.dataSize = bucket->dataSize;

                /* update statistics */
                header->numUsedBuckets--;
                header->numEvictingBuckets++;
                break;
            }
        }
        else if (--trycounter == 0){
            THROW(GopherwoodSharedMemException,
                  "[SharedMemoryContext::acquireBlock] statistic incorrect, there should be %d used"
                          "buckets.", header->numUsedBuckets);
        }
    }

    LOG(DEBUG1, "[SharedMemoryContext]   |"
              "Start evicting bucketId %d, FileId %s, BlockId %d",
        info.bucketId, info.fileId.toString().c_str(), info.blockId);
    printStatistics();
    return info;
}

/* return code:
 * 0 -- evict finish successfully
 * 1 -- the evicted block has been deleted during the eviction
 * 2 -- the evicted bucket has been activated by it's file owner, give up this one
 */
int SharedMemoryContext::evictBucketFinish(int32_t bucketId, int16_t activeId, FileId fileId, int isWrite) {
    int rc = 0;

    /* reset activestatus evict info  */
    activeStatus[activeId].evictFileId.reset();
    activeStatus[activeId].fileBlockIndex = InvalidBlockId;
    activeStatus[activeId].unsetEvicting();

    if (activeStatus[activeId].isEvictBucketStolen()) {
        activeStatus[activeId].unsetBucketStolen();
        return 2;
    }

    /* check whether the evicted block been deleted during evicting */
    if (buckets[bucketId].isDeletedBucket()) {
        rc = 1;
    }

    /* clear bucket info */
    buckets[bucketId].reset();
    buckets[bucketId].setBucketActive();

    buckets[bucketId].fileId = fileId;
    if (isWrite) {
        buckets[bucketId].markWrite(activeId);
    } else {
        buckets[bucketId].markRead(activeId);
    }

    /* update statistics */
    header->numEvictingBuckets--;
    header->numActiveBuckets++;

    LOG(DEBUG1, "[SharedMemoryContext]   |"
              "Bucket %d evict finished.", bucketId);
    printStatistics();

    return rc;
}

/* Mark a block is loading by me
 * return true -- I've marked the block loading
 * return false -- The block is loading by some others
 * */
bool SharedMemoryContext::markBucketLoading(int32_t bucketId, int32_t blockId, int16_t activeId, FileId fileId) {
    assert(buckets[bucketId].isActiveBucket());
    assert(buckets[bucketId].fileId == fileId);

    for (int i = 0; i < header->numMaxActiveStatus; i++) {
        if (activeStatus[i].isLoading() &&
            activeStatus[i].fileId == fileId &&
            activeStatus[i].fileBlockIndex == blockId) {
            LOG(DEBUG1, "[SharedMemoryContext]   |"
                    "FileId %s, BlockId %d is loading by activeStatus %d, pid %d",
                fileId.toString().c_str(), blockId, i, activeStatus[i].pid);
            return false;
        }
    }

    /* update the bucket info */
    buckets[bucketId].fileBlockIndex = blockId;
    buckets[bucketId].setBucketLoading();
    buckets[bucketId].evictLoadActiveId = activeId;

    /* fill ActiveStatus evict info */
    activeStatus[activeId].fileBlockIndex = blockId;
    activeStatus[activeId].setLoading();

    LOG(DEBUG1, "[SharedMemoryContext]   |"
            "Start loading bucketId %d, FileId %s, BlockId %d",
        bucketId, fileId.toString().c_str(), blockId);

    return true;
}

void SharedMemoryContext::markLoadFinish(int32_t bucketId, int16_t activeId, FileId fileId) {
    assert(buckets[bucketId].isActiveBucket());
    assert(buckets[bucketId].isLoadingBucket());
    /* update the bucket info */
    buckets[bucketId].setBucketLoadFinish();

    /* clear ActiveStatus loading info */
    activeStatus[activeId].fileBlockIndex = InvalidBlockId;
    activeStatus[activeId].unsetLoading();
}

bool SharedMemoryContext::isBlockLoading(FileId fileId, int32_t blockId) {
    for (int i = 0; i < header->numMaxActiveStatus; i++) {
        if (activeStatus[i].isLoading() &&
            activeStatus[i].fileId == fileId &&
            activeStatus[i].fileBlockIndex == blockId) {
            return true;
        }
    }
    return false;
}

/* Transit Bucket State from 1 to 0 */
void SharedMemoryContext::releaseBuckets(std::list<Block> &blocks) {
    for (Block block : blocks) {
        int32_t bucketId = block.bucketId;
        if (buckets[bucketId].isActiveBucket()) {
            buckets[bucketId].reset();
            buckets[bucketId].setBucketFree();
            /* update statistics */
            header->numActiveBuckets--;
            header->numFreeBuckets++;
        } else {
            THROW(
                    GopherwoodSharedMemException,
                    "[SharedMemoryContext::releaseBlock] state of bucket %d is not Active",
                    bucketId);
        }
    }
    LOG(DEBUG1, "[SharedMemoryContext]   |"
              "Released %lu blocks.", blocks.size());
    printStatistics();
}

/* Activate a block from status 2 to 1. No need to evict because it's just the same File.
 * return 0  -- The bucket is not activated by current process
 * return 1  -- The bucket is activated by current process
 * return -1 -- error
 * */
int SharedMemoryContext::activateBucket(FileId fileId, Block &block, int16_t activeId, bool isWrite) {
    int rc = -1;
    int32_t bucketId = block.bucketId;

    if (buckets[bucketId].fileId != fileId ||
        buckets[bucketId].fileBlockIndex != block.blockId) {
        THROW(GopherwoodSharedMemException,
              "[SharedMemoryContext::activateBlock] File Id mismatch, expect fileId = %lu-%u, "
                      "current bucket fileId=%lu-%u",
              fileId.hashcode, fileId.collisionId,
              buckets[bucketId].fileId.hashcode, buckets[bucketId].fileId.collisionId
        );
    }

    if (buckets[bucketId].isUsedBucket()) {
        buckets[bucketId].setBucketActive();
        LOG(DEBUG1, "[SharedMemoryContext]   |"
                  "File %s bucket %d activated. state %d",
            fileId.toString().c_str(), bucketId, buckets[bucketId].flags);
        if (isWrite) {
            buckets[bucketId].markWrite(activeId);
            LOG(DEBUG1, "[SharedMemoryContext]   |"
                      "Mark Write-Active activeId %d, bucketId %d", activeId, bucketId);
        } else {
            buckets[bucketId].markRead(activeId);
            LOG(DEBUG1, "[SharedMemoryContext]   |"
                      "Mark Read-Active activeId %d, bucketId %d", activeId, bucketId);
        }

        if (buckets[bucketId].isEvictingBucket()) {
            int32_t evictId = buckets[bucketId].evictLoadActiveId;
            if (activeStatus[evictId].evictFileId == buckets[bucketId].fileId &&
                activeStatus[evictId].fileBlockIndex == buckets[bucketId].fileBlockIndex) {
                activeStatus[evictId].setBucketStolen();
            } else {
                THROW(GopherwoodSharedMemException,
                      "[activateBucket] The activeStatus %d is not evicting file %s block %d",
                      evictId, buckets[bucketId].fileId.toString().c_str(), buckets[bucketId].fileBlockIndex);
            }
            header->numEvictingBuckets--;
            header->numActiveBuckets++;
        } else {
            header->numUsedBuckets--;
            header->numActiveBuckets++;
        }
        rc = 1;
    } else if (buckets[bucketId].isActiveBucket()) {
        if (isWrite) {
            buckets[bucketId].markWrite(activeId);
        } else {
            buckets[bucketId].markRead(activeId);
        }

        rc = 0;
        LOG(DEBUG1, "[SharedMemoryContext]   |"
                "File %s bucket %d already activated by others. state %d",
                fileId.toString().c_str(), bucketId, buckets[bucketId].flags);
    } else {
        THROW(GopherwoodSharedMemException,
              "[SharedMemoryContext::activateBlock] Dead Zone, get out!");
        rc = -1;
    }
    printStatistics();
    return rc;
}

/* Transit Bucket State from 1 to 2 */
std::vector<Block>
SharedMemoryContext::inactivateBuckets(std::vector<Block> &blocks, FileId fileId, int16_t activeId, bool isWrite) {
    std::vector<Block> res;
    for (Block b : blocks) {
        if (buckets[b.bucketId].isActiveBucket()) {
            buckets[b.bucketId].fileId = fileId;
            buckets[b.bucketId].fileBlockIndex = b.blockId;
            buckets[b.bucketId].usageCount += b.usageCount;
            if (isWrite) {
                buckets[b.bucketId].unmarkWrite(activeId);
            } else {
                buckets[b.bucketId].unmarkRead(activeId);
            }

            /* only return 1->2 blocks for manifest logging */
            if (buckets[b.bucketId].noActiveReadWrite()) {
                buckets[b.bucketId].setBucketUsed();
                b.state = BUCKET_USED;
                res.push_back(b);
                /* update statistics */
                header->numActiveBuckets--;
                header->numUsedBuckets++;
            }
        } else {
            THROW(GopherwoodSharedMemException,
                  "[SharedMemoryContext] state of bucket %d is not Active when trying to release block",
                  b.bucketId);
        }
    }
    LOG(DEBUG1, "[SharedMemoryContext]   |"
              "Inactivated %lu blocks.", blocks.size());
    printStatistics();
    return res;
}

void SharedMemoryContext::updateActiveFileInfo(std::vector<Block> &blocks, FileId fileId) {
    for (uint32_t i = 0; i < blocks.size(); i++) {
        Block b = blocks[i];
        if (buckets[b.bucketId].fileId == fileId) {
            buckets[b.bucketId].fileBlockIndex = b.blockId;
        } else {
            THROW(GopherwoodSharedMemException,
                  "[SharedMemoryContext] FileId mismatch, expectging %s, actually %s",
                  fileId.toString().c_str(), buckets[b.bucketId].fileId.toString().c_str());
        }
    }
}

void SharedMemoryContext::deleteBlocks(std::vector<Block> &blocks, FileId fileId) {
    for (uint32_t i = 0; i < blocks.size(); i++) {
        Block b = blocks[i];

        /* set free if the bucket still in used status */
        if (buckets[b.bucketId].isUsedBucket() && buckets[b.bucketId].fileId == fileId) {
            buckets[b.bucketId].reset();
            buckets[b.bucketId].setBucketFree();
            /* update statistics */
            header->numUsedBuckets--;
            header->numFreeBuckets++;
        }
            /* mark deleted if it's been evicting by someone */
        else if (buckets[b.bucketId].isEvictingBucket() && buckets[b.bucketId].fileId == fileId) {
            buckets[b.bucketId].setBucketDeleted();
        } else {
            THROW(GopherwoodSharedMemException,
                  "[SharedMemoryContext] Bucket %d status mismatch!", b.bucketId);
        }
    }
    LOG(DEBUG1, "[SharedMemoryContext]   |"
              "Deleted %lu blocks.", blocks.size());
    printStatistics();
}

void SharedMemoryContext::updateBucketDataSize(int32_t bucketId, int64_t size, FileId fileId, int16_t activeId) {
    if (size > Configuration::LOCAL_BUCKET_SIZE ||
        bucketId < 0 || bucketId > Configuration::NUMBER_OF_BLOCKS ||
        buckets[bucketId].fileId != fileId ||
        !buckets[bucketId].isActiveBucket() ||
        !buckets[bucketId].hasActiveId(activeId)) {
        THROW(GopherwoodSharedMemException,
              "[SharedMemoryContext] Bucket %d status mismatch!", bucketId);
    }
    if (buckets[bucketId].dataSize < size)
        buckets[bucketId].dataSize = size;
}

int64_t SharedMemoryContext::getBucketDataSize(int32_t bucketId, FileId fileId, int32_t blockId) {
    if (bucketId < 0 || bucketId > Configuration::NUMBER_OF_BLOCKS ||
        buckets[bucketId].fileId != fileId ||
        buckets[bucketId].fileBlockIndex != blockId) {
        THROW(GopherwoodSharedMemException,
              "[SharedMemoryContext] Bucket %d status mismatch! "
                      "<input> fileId %s blockId %d "
                      "<ShareMem> fileId %s blockId %d ",
              bucketId, fileId.toString().c_str(), blockId,
              buckets[bucketId].fileId.toString().c_str(),
              buckets[bucketId].fileBlockIndex);
    }
    return buckets[bucketId].dataSize;
}

int SharedMemoryContext::calcDynamicQuotaNum() {
    return Configuration::getCurQuotaSize();
}


std::string &SharedMemoryContext::getWorkDir() {
    return workDir;
}

int32_t SharedMemoryContext::getNumMaxActiveStatus() {
    return header->numMaxActiveStatus;
}

int32_t SharedMemoryContext::getFreeBucketNum() {
    return header->numFreeBuckets;
}

int32_t SharedMemoryContext::getActiveBucketNum() {
    return header->numActiveBuckets;
}

int32_t SharedMemoryContext::getUsedBucketNum() {
    return header->numUsedBuckets;
}

int32_t SharedMemoryContext::getEvictingBucketNum() {
    return header->numEvictingBuckets;
}

void SharedMemoryContext::printStatistics() {
    LOG(DEBUG1, "[SharedMemoryContext]   |"
              "Statistics: free %d, active %d, used %d, evicting %d",
        header->numFreeBuckets, header->numActiveBuckets, header->numUsedBuckets, header->numEvictingBuckets);
}

SharedMemoryContext::~SharedMemoryContext() {
    if (mLockFD > 0) {
        close(mLockFD);
        mLockFD = -1;
    }
}

}
}
