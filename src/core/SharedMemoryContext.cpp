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

int SharedMemoryContext::regist(int pid, FileId fileId, bool isWrite, bool isDelete) {
    int activeId = -1;
    for (int i = 0; i < header->numMaxActiveStatus; i++) {
        /* validation */
        if (activeStatus[i].fileId == fileId) {
            /* file is opening by someone */
            if (isDelete) {
                THROW(GopherwoodSharedMemException,
                      "[SharedMemoryContext::regist] The file is still openning by %d",
                      activeStatus[i].pid);
            }/* file is deleting by someone */
            else if (!isDelete && activeStatus[i].isForDelete()) {
                THROW(GopherwoodSharedMemException,
                      "[SharedMemoryContext::regist] The file is still deleting by %d",
                      activeStatus[i].pid);
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

    LOG(INFO, "[SharedMemoryContext]   |"
              "Regist file %s, activeId=%d, pid=%d",
        fileId.toString().c_str(),activeId, activeStatus[activeId].pid);
    return activeId;
}

int SharedMemoryContext::unregist(int activeId, int pid) {
    if (activeId < 0 || activeId >= header->numMaxActiveStatus) {
        return -1;
    }
    LOG(INFO, "[SharedMemoryContext]   |"
              "Unregist File %s, activeId=%d, pid=%d",
        activeStatus[activeId].fileId.toString().c_str(),
        activeId, activeStatus[activeId].pid);
    if (activeStatus[activeId].pid == pid) {
        activeStatus[activeId].reset();
        return 0;
    }
    return -1;
}

/* Check if all ActiveStatus of a given FileId are unregisted.
 * This is called by ActiveStatus close, manifest log will be truncated if
 * this function returns true */
bool SharedMemoryContext::isFileOpening(FileId fileId) {
    for (int i = 0; i < Configuration::MAX_CONNECTION; i++) {
        if (activeStatus[i].fileId == fileId) {
            return true;
        }
    }
    LOG(INFO, "[SharedMemoryContext]   |"
              "No more active status of file %s",
        fileId.toString().c_str());
    return false;
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

    LOG(INFO, "[SharedMemoryContext]   |"
              "Acquired %lu free buckets.", res.size());
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
BlockInfo SharedMemoryContext::markBucketEvicting(int activeId) {
    BlockInfo info;
    bool found = false;

    /* pick up from used buckets */
    for (int32_t i = 0; i < header->numBuckets; i++) {
        if (buckets[i].isUsedBucket() && !buckets[i].isEvictingBucket()) {
            buckets[i].setBucketEvicting();
            buckets[i].evictActiveId = activeId;

            /* fill ActiveStatus evict info */
            activeStatus[activeId].evictFileId = buckets[i].fileId;
            activeStatus[activeId].fileBlockIndex = buckets[i].fileBlockIndex;
            activeStatus[activeId].setEvicting();

            /* fill result BlockInfo */
            info.fileId = buckets[i].fileId;
            info.blockId = buckets[i].fileBlockIndex;
            info.bucketId = i;
            info.isLocal = true;
            info.offset = InvalidBlockOffset;

            /* update statistics */
            header->numUsedBuckets--;
            header->numEvictingBuckets++;
            found = true;
            break;
        }
    }

    if (!found) {
        THROW(GopherwoodSharedMemException,
              "[SharedMemoryContext::acquireBlock] statistic incorrect, there should be %d used"
                      "buckets.", header->numUsedBuckets);
    }

    LOG(INFO, "[SharedMemoryContext]   |"
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
int SharedMemoryContext::evictBucketFinish(int32_t bucketId, int activeId, FileId fileId, int isWrite) {
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

    LOG(INFO, "[SharedMemoryContext]   |"
              "Bucket %d evict finished.", bucketId);
    printStatistics();

    return rc;
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
    LOG(INFO, "[SharedMemoryContext]   |"
              "Released %lu blocks.", blocks.size());
    printStatistics();
}

/* Activate a block from status 2 to 1. No need to evict because it's just the same File.
 * return true if the bucket is activated by current process */
bool SharedMemoryContext::activateBucket(FileId fileId, Block &block, int activeId, bool isWrite) {
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
        LOG(INFO, "[SharedMemoryContext]   |"
                  "File %s bucket %d activated. state %d",
            fileId.toString().c_str(), bucketId, buckets[bucketId].flags);
        if (isWrite) {
            buckets[bucketId].markWrite(activeId);
            LOG(INFO, "[SharedMemoryContext]   |"
                      "Mark Write-Active activeId %d, bucketId %d", activeId, bucketId);
        } else {
            buckets[bucketId].markRead(activeId);
            LOG(INFO, "[SharedMemoryContext]   |"
                      "Mark Read-Active activeId %d, bucketId %d", activeId, bucketId);
        }

        if (buckets[bucketId].isEvictingBucket()) {
            int32_t evictId = buckets[bucketId].evictActiveId;
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
        printStatistics();
        return true;
    } else if (buckets[bucketId].isActiveBucket()) {
        if (isWrite) {
            buckets[bucketId].markWrite(activeId);
        } else {
            buckets[bucketId].markRead(activeId);
        }
        printStatistics();
        return false;
    } else {
        THROW(GopherwoodSharedMemException,
              "[SharedMemoryContext::activateBlock] Dead Zone, get out!");
        return false;
    }
}

/* Transit Bucket State from 1 to 2 */
std::vector<Block>
SharedMemoryContext::inactivateBuckets(std::vector<Block> &blocks, FileId fileId, int activeId, bool isWrite) {
    std::vector<Block> res;
    for (uint32_t i = 0; i < blocks.size(); i++) {
        Block b = blocks[i];
        if (buckets[b.bucketId].isActiveBucket()) {
            buckets[b.bucketId].fileId = fileId;
            buckets[b.bucketId].fileBlockIndex = b.blockId;
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
    LOG(INFO, "[SharedMemoryContext]   |"
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
    LOG(INFO, "[SharedMemoryContext]   |"
              "Deleted %lu blocks.", blocks.size());
    printStatistics();
}

int SharedMemoryContext::calcDynamicQuotaNum() {
    return Configuration::CUR_QUOTA_SIZE;
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
    LOG(INFO, "[SharedMemoryContext]   |"
              "Statistics: free %d, active %d, used %d, evicting %d",
        header->numFreeBuckets, header->numActiveBuckets, header->numUsedBuckets, header->numEvictingBuckets);
}

SharedMemoryContext::~SharedMemoryContext() {

}

}
}
