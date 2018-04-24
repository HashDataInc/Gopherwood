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
#include "common/Configuration.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Logger.h"
#include "common/Thread.h"
#include "core/ActiveStatus.h"
#include "file/FileSystem.h"

namespace Gopherwood {
namespace Internal {

ActiveStatus::ActiveStatus(FileId fileId,
                           shared_ptr<SharedMemoryContext> sharedMemoryContext,
                           bool isCreate,
                           bool isSequence,
                           ActiveStatusType type,
                           int localSpaceFD) :
        mFileId(fileId),
        mSharedMemoryContext(sharedMemoryContext) {
    mIsWrite = (type == ActiveStatusType::writeFile);
    mIsDelete = (type == ActiveStatusType::deleteFile);
    mIsSequence = isSequence;
    mShouldDestroy = false;

    /* check file exist if not creating a new file */
    std::string manifestFileName = Manifest::getManifestFileName(mSharedMemoryContext->getWorkDir(), mFileId);
    if (!isCreate && access(manifestFileName.c_str(), F_OK) == -1) {
        THROW(GopherwoodInvalidParmException,
              "[ActiveStatus::ActiveStatus] File does not exist %s",
              manifestFileName.c_str());
    }

    uint32_t quotaSize;
    /* for sequence read/write, used block should be inactivated promptly.
     * Thus we just set the quota to pre-allocate-size */
    if (mIsSequence)
        quotaSize = Configuration::getCurQuotaSize() > Configuration::PRE_ALLOCATE_BUCKET_NUM ?
                    Configuration::PRE_ALLOCATE_BUCKET_NUM :
                    Configuration::getCurQuotaSize();
    else
        quotaSize = Configuration::getCurQuotaSize();

    mLRUCache = shared_ptr<LRUCache<int, int>>(new LRUCache<int, int>(quotaSize));
    mManifest = shared_ptr<Manifest>(new Manifest(manifestFileName));
    mOssWorker = shared_ptr<OssBlockWorker>(new OssBlockWorker(FileSystem::OSS_CONTEXT, localSpaceFD));

    /* init file related info */
    mPos = 0;
    mEof = 0;
    mBucketSize = Configuration::LOCAL_BUCKET_SIZE;

    SHARED_MEM_BEGIN
        registInSharedMem();
    SHARED_MEM_END

    /* init statistics */
    mNumEvicted = 0;
    mNumLoaded = 0;
    mNumActivated = 0;
}

/* Shared Memroy activeStatus field will maintain all connected files */
void ActiveStatus::registInSharedMem() {
    mActiveId = mSharedMemoryContext->regist(getpid(), mFileId, mIsWrite, mIsDelete);
    if (mActiveId == -1) {
        THROW(GopherwoodSharedMemException,
              "[ActiveStatus::registInSharedMem] Exceed max connection limitation %d",
              mSharedMemoryContext->getNumMaxActiveStatus());
    }
    LOG(DEBUG1, "[ActiveStatus]          |"
            "Registered successfully, ActiveID=%d, PID=%d", mActiveId, getpid());
}

void ActiveStatus::unregistInSharedMem(bool isCancel) {
    if (mActiveId == -1)
        return;

    int rc = mSharedMemoryContext->unregist(mActiveId, getpid(), &mShouldDestroy);
    if (rc != 0) {
        mSharedMemoryContext->unlock();
        THROW(GopherwoodSharedMemException,
              "[ActiveStatus::unregistInSharedMem] connection info mismatch with SharedMem ActiveId=%d, PID=%d",
              mActiveId, getpid());
    }
    if (isCancel)
        mShouldDestroy = true;

    LOG(DEBUG1, "[ActiveStatus]          |"
            "Unregistered successfully, ActiveID=%d, PID=%d", mActiveId, getpid());
    mActiveId = -1;
}

int64_t ActiveStatus::getPosition() {
    return mPos;
}

void ActiveStatus::setPosition(int64_t pos) {
    mPos = pos;
    if (mPos > mEof) {
        mEof = mPos;
        updateCurBlockSize();
    }
    LOG(DEBUG1, "[ActiveStatus]          |"
            "Update ActiveStatus position, pos=%ld, eof=%ld",
        mPos, mEof);
}

int64_t ActiveStatus::getEof() {
    return mEof;
}

Block ActiveStatus::getCurBlock() {
    return mBlockArray[mPos / mBucketSize];
}

int32_t ActiveStatus::getNumBlocks() {
    return mBlockArray.size();
}

int64_t ActiveStatus::getCurBlockOffset() {
    return mPos % mBucketSize;
}

int32_t ActiveStatus::getCurQuota() {
    return mLRUCache->maxSize();
}

int32_t ActiveStatus::getNumAcquiredBuckets() {
    return mLRUCache->size() + mPreAllocatedBuckets.size();
}

/* update the Eof in in current SharedMemBucket */
void ActiveStatus::updateCurBlockSize() {
    assert(mEof > 0);
    int64_t eofBeforeCatchUp = mEof;

    SHARED_MEM_BEGIN
        if (eofBeforeCatchUp == mEof) {
            int numBlocks = mBlockArray.size();
            int32_t endBucketId = mBlockArray[numBlocks-1].bucketId;
            int64_t blockDataSize = mEof - (numBlocks-1) * Configuration::LOCAL_BUCKET_SIZE;

            LOG(DEBUG1, "[ActiveStatus]          |"
                    "Update Current bucket Eof, BucketId=%d, BlockEof=%ld",
                endBucketId, blockDataSize);
            mSharedMemoryContext->updateBucketDataSize(endBucketId, blockDataSize, mFileId, mActiveId);
        } else if (eofBeforeCatchUp < mEof) {
            LOG(DEBUG1, "[ActiveStatus]          |"
                    "updateCurBlockSize: Someone have write beyond me!");
        } else {
            THROW(GopherwoodInternalException,
                  "[ActiveStatus::updateCurBlockSize] Unexpected File Eof");
        }
    SHARED_MEM_END

}

std::string ActiveStatus::getManifestFileName(FileId fileId) {
    std::stringstream ss;
    ss << mSharedMemoryContext->getWorkDir() << Configuration::MANIFEST_FOLDER << '/' << mFileId.hashcode << '-'
       << mFileId.collisionId;
    return ss.str();
}

bool ActiveStatus::isMyActiveBlock(int blockId) {
    return mLRUCache->exists(blockId);
}

/* [IMPORTANT] This is the main entry point of adjusting active status. OutpuStream/InputStream
 * will call this function to write/read to multi blocks. When mPos reaches block not activated
 * by current file instance, ActiveStatus will adjust the block status.*/
BlockInfo ActiveStatus::getCurBlockInfo() {
    int curBlockId = mPos / mBucketSize;

    /* adjust the active block status */
    adjustActiveBlock(curBlockId);

    /* update the usage count */
    mBlockArray[curBlockId].usageCount++;

    /* build the block info */
    BlockInfo info;
    Block block = getCurBlock();
    info.fileId = mFileId;
    info.blockId = block.blockId;
    info.bucketId = block.bucketId;
    info.isLocal = block.isLocal;
    info.offset = getCurBlockOffset();
    return info;
}

void ActiveStatus::getStatistics(GWFileInfo *fileInfo) {
    fileInfo->fileSize = getEof();
    fileInfo->maxQuota = mLRUCache->maxSize();
    fileInfo->curQuota = getNumAcquiredBuckets();
    fileInfo->numBlocks = mBlockArray.size();
    fileInfo->numActivated = mNumActivated;
    fileInfo->numEvicted = mNumEvicted;
    fileInfo->numLoaded = mNumLoaded;
}

void ActiveStatus::adjustActiveBlock(int curBlockId) {
    if (curBlockId + 1 > getNumBlocks()) {
        extendOneBlock();
    } else if (!isMyActiveBlock(curBlockId)) {
        int numToPreActivate = 4;
        /**/
        while(numToPreActivate > 0) {
            activateBlock(curBlockId);
            activateBlockWithPreload(curBlockId);
            numToPreActivate--;
        }
    } else {
        if (!mLRUCache->exists(curBlockId)) {
            THROW(GopherwoodException, "[ActiveStatus] block active status mismatch!");
        }
    }
    // else is loading, then wait
}

/* All block activation should follow these steps:
 * 1. Check shared memory for the current quota
 * 2(a). If still have quota available, and have 0 or 2 available
 *       Then -> acquire more buckets for preAllocatedBlocks
 * 2(b). If still have quota available, and no 0 or 2 available
 *       Then -> play with current owned buckets
 * 3(a). If quota equal to current active block num, and have 0 available
 *       Then -> inactivate blocks from LRU first, then acquire new blocks
 * 3(b). If quota equal to current active block num, and no 0 available
 *       Then -> play with current owned buckets
 * 4. If quota smaller than current active block num,
 *       Then -> release blocks and use own quota
 * Notes: When got chance to acquire new blocks, active status will try to
 *        pre acquire a number of buckets to reduce the Shared Memory contention. */
void ActiveStatus::acquireNewBlocks() {
    std::vector<Block> blocksForLog;
    std::vector<int32_t> newBuckets;
    BlockInfo evictBlockInfo;
    evictBlockInfo.bucketId = -1;
    bool evicting = false;

    uint32_t numToAcquire = 0;
    uint32_t numToInactivate = 0;

    uint32_t numFreeBuckets;
    uint32_t numUsedBuckets;
    uint32_t numAvailable;

    SHARED_MEM_BEGIN
        uint32_t newQuota = mSharedMemoryContext->calcDynamicQuotaNum();
        numFreeBuckets = mSharedMemoryContext->getFreeBucketNum();
        numUsedBuckets = mSharedMemoryContext->getUsedBucketNum();
        numAvailable = numFreeBuckets + numUsedBuckets;
        assert(newQuota > 0);

        /****************************************************************
         * Step0: adjust the new quota size depending on the hint policy
         ****************************************************************/
        if (mIsSequence) {
            newQuota = newQuota > Configuration::PRE_ALLOCATE_BUCKET_NUM ?
                            Configuration::PRE_ALLOCATE_BUCKET_NUM :
                            newQuota;
        }

        /************************************************
         * Step1: Determine the acquire policy first
         * NOTE: The preAllocateList size is 0
         ************************************************/
        if ((uint32_t)getNumAcquiredBuckets() < newQuota) {
            if (numAvailable > 0) {
                /* 2(a) acquire more buckets for preAllocatedBlocks */
                uint32_t tmpAcquire = newQuota - mLRUCache->size();
                if (tmpAcquire > Configuration::PRE_ALLOCATE_BUCKET_NUM) {
                    tmpAcquire = Configuration::PRE_ALLOCATE_BUCKET_NUM;
                }
                numToAcquire = numAvailable > tmpAcquire ? tmpAcquire : numAvailable;
                /* it might exceed quota after acquired new buckets */
                numToInactivate = (mLRUCache->size() + numToAcquire) > newQuota ?
                                   mLRUCache->size() + numToAcquire - newQuota : 0;
            } else {
                /* 2(b) play with current owned buckets
                 * evict one block at a time. This will make sure the inactivated block is
                 * evicted by myself */
                numToInactivate = 1;
                numToAcquire = 1;
            }
        } else if ((uint32_t)getNumAcquiredBuckets() == newQuota) {
            if (numFreeBuckets > 0) {
                /* 3(a) inactivate blocks from LRU first, then acquire new blocks */
                uint32_t tmpAcquire = newQuota > Configuration::PRE_ALLOCATE_BUCKET_NUM ?
                                      Configuration::PRE_ALLOCATE_BUCKET_NUM : newQuota;
                numToAcquire = numFreeBuckets > tmpAcquire ? tmpAcquire : numFreeBuckets;
                numToInactivate = numToAcquire;
            } else {
                /* 3(b) play with current owned buckets */
                numToInactivate = 1;
                numToAcquire = 1;
            }
        } else {
            /* 4 release blocks and use own quota */
            numToInactivate = mLRUCache->size() - newQuota + 1;
            numToAcquire = 1;
        }
        LOG(DEBUG1, "[ActiveStatus]          |"
                "Calculate new quota size, newQuota=%u, curQuota=%ld, numAvailables=%d, "
                "inactivate=%d, acquire %d", newQuota, mLRUCache->size(), numAvailable,
            numToInactivate, numToAcquire);

        /**************************************************************
         * Step2: inactive/get free buckets/start evict 1st used bucket
         * NOTE: Only get free buckets since 3(a) is determined
         * by numFreeBuckets. We should do this consistently
         * before release SharedMemory lock.
         **************************************************************/
        /* release first */
        if (numToInactivate > 0) {
            std::vector<Block> blocksToInactivate;
            std::vector<int> blockIds = mLRUCache->removeNumOfKeys(numToInactivate);

            for (int blockId : blockIds) {
                blocksToInactivate.push_back(mBlockArray[blockId]);
            }
            std::vector<Block> turedToUsedBlocks =
                    mSharedMemoryContext->inactivateBuckets(blocksToInactivate,
                                                            mFileId,
                                                            mActiveId,
                                                            mIsWrite);

            /* update block status*/
            for (Block b : turedToUsedBlocks) {
                mBlockArray[b.blockId].state = b.state;

                /* the ending block been inactivated, flush EoF */
                if ((uint32_t)b.blockId + 1 == mBlockArray.size()){
                    /* update EoF from SharedMemory */
                    getSharedMemEof();
                    /* add the Eof log */
                    RecOpaque opaque;
                    opaque.updateEof.eof = mEof;
                    mManifest->logUpdateEof(opaque);
                }
            }
            /* log inactivate buckets */
            mManifest->logInactivateBucket(turedToUsedBlocks);
        }

        /* after released a number of blocks, we can set the new Quota */
        std::vector<int> overflowBlocks = mLRUCache->adjustSize(newQuota);
        if (overflowBlocks.size() > 0) {
            THROW(GopherwoodInternalException, "We should have inactivated these blocks!");
        }

        /* acquire new buckets */
        if (numToAcquire > 0) {
            uint32_t numAcqurieFree = numToAcquire > numFreeBuckets ? numFreeBuckets : numToAcquire;
            int numToEvict = numToAcquire - numAcqurieFree;

            newBuckets = mSharedMemoryContext->acquireFreeBucket(mActiveId, numAcqurieFree, mFileId, mIsWrite);
            /* add free buckets to preAllocatedBlocks */
            for (int32_t bucketId : newBuckets) {
                LOG(DEBUG1, "[ActiveStatus]          |"
                        "Pre-allocate bucket %d to pre-allocated bucket array.", bucketId);
                Block newBlock(bucketId,
                               InvalidBlockId,
                               LocalBlock,
                               BUCKET_ACTIVE);
                blocksForLog.push_back(newBlock);
                mPreAllocatedBuckets.push_back(newBlock);
                /* update statistics */
                mNumActivated++;
            }
            numToAcquire -= numAcqurieFree;

            /* Manifest Log */
            mManifest->logAcquireNewBlock(blocksForLog);
            blocksForLog.clear();

            /* start evict 1st used bucket */
            if (numToEvict > 0) {
                evictBlockInfo = mSharedMemoryContext->markBucketEvicting(mActiveId);
                evicting = true;
            }
        }
    SHARED_MEM_END

    /************************************************
     * Step2: Loop to get used buckets
     * 1. check if there is any free buckets again
     * 2. evict used buckets
     ************************************************/
    while (numToAcquire > 0 || evicting) {
        /* evict the bucket */
        if (evicting) {
            mOssWorker->writeBlock(evictBlockInfo);
        }

        SHARED_MEM_BEGIN
            /* mark evict finish */
            if (evicting) {
                int rc = mSharedMemoryContext->evictBucketFinish(evictBlockInfo.bucketId,
                                                                 mActiveId,
                                                                 mFileId,
                                                                 mIsWrite);
                if (rc == 0 || rc == 1) {
                    Block newBlock(evictBlockInfo.bucketId, InvalidBlockId, LocalBlock, BUCKET_ACTIVE);
                    LOG(DEBUG1, "[ActiveStatus]          |"
                            "Add block %d to pre-allocated bucket array.",
                        newBlock.bucketId);
                    blocksForLog.push_back(newBlock);
                    mPreAllocatedBuckets.push_back(newBlock);
                    evicting = false;
                    numToAcquire--;
                    /* update statistics */
                    mNumActivated++;
                }

                /* add log to the evicted file */
                if (rc == 0) {
                    logEvictBlock(evictBlockInfo);
                    mNumEvicted ++;
                }

                /* 1: the evicted block has been deleted during eviction
                 * 2: the evicted bucket has been activated by it's file owner, give up this one*/
                if (rc == 1 || rc == 2) {
                    /* the evicted bucket has been activated by it's file owner, give up this one */
                    mOssWorker->deleteBlock(evictBlockInfo);
                }
            }

            /* acquire free buckets */
            numFreeBuckets = mSharedMemoryContext->getFreeBucketNum();
            uint32_t numAcqurieFree = numToAcquire > numFreeBuckets ? numFreeBuckets : numToAcquire;

            if (numAcqurieFree > 0) {
                newBuckets = mSharedMemoryContext->acquireFreeBucket(mActiveId, numAcqurieFree, mFileId, mIsWrite);
                /* add free buckets to preAllocatedBlocks */
                for (int32_t bucketId: newBuckets) {
                    LOG(DEBUG1, "[ActiveStatus]          |"
                            "Add block %d to pre-allocated bucket array.", bucketId);
                    Block newBlock(bucketId,
                                   InvalidBlockId,
                                   LocalBlock,
                                   BUCKET_ACTIVE);
                    blocksForLog.push_back(newBlock);
                    mPreAllocatedBuckets.push_back(newBlock);
                    /* update statistics */
                    mNumActivated++;
                }
                numToAcquire -= numAcqurieFree;
            }

            /* Manifest Log */
            mManifest->logAcquireNewBlock(blocksForLog);
            blocksForLog.clear();

            /* start evict next bucket */
            if (numToAcquire > 0) {
                evictBlockInfo = mSharedMemoryContext->markBucketEvicting(mActiveId);
                evicting = true;
            }
        SHARED_MEM_END
    }
    if (mPreAllocatedBuckets.size() <= 0) {
        THROW(GopherwoodException, "Did not acquire any block!");
    }
}

/* Extend the file to create a new block, the block will get bucket from the
 * pre allocated bucket array */
void ActiveStatus::extendOneBlock() {
    std::vector<Block> blocksModified;

    if (mPreAllocatedBuckets.size() == 0) {
        acquireNewBlocks();
    }

    /* build the block */
    Block b = mPreAllocatedBuckets.front();
    mPreAllocatedBuckets.pop_front();
    b.blockId = getNumBlocks();

    /* add to block array */
    mBlockArray.push_back(b);

    /* add to LRU cache */
    mLRUCache->put(b.blockId, b.bucketId);

    /* prepare for log */
    blocksModified.push_back(b);

    SHARED_MEM_BEGIN
        /* update file info to shared memory */
        mSharedMemoryContext->updateActiveFileInfo(blocksModified, mFileId);
        /* Manifest Log */
        RecOpaque opaque;
        opaque.extendBlock.eof = mEof;
        mManifest->logExtendBlock(blocksModified, opaque);
    SHARED_MEM_END
    LOG(DEBUG1, "[ActiveStatus]          |"
            "ExtendOneBlock blockId=%d, bucketId=%d",
        b.blockId, b.bucketId);
}

/* active a block back to my control */
void ActiveStatus::activateBlock(int blockId) {
    bool loadBlock = false;
    int rc = -1;

    /* to eliminate the Shared Memory consistent issues,
     * We acquire new blocks first, then we activate the block.
     * Actually only loadBlock need acquire new block, but we
     * still acquire for all cases to simplify the logic. */
    if (mPreAllocatedBuckets.size() == 0) {
        acquireNewBlocks();
    }

    /* Main logic to activate the block */
    SHARED_MEM_BEGIN
        /* after catched up the log, we can handle cases in a consistent state */
        if (!mBlockArray[blockId].isLocal) {
            loadBlock = true;
            /* build the block */
            Block block = mPreAllocatedBuckets.front();
            mPreAllocatedBuckets.pop_front();
            block.blockId = blockId;

            /* update Shared Memory */
            //mSharedMemoryContext->markBucketLoading(block, mActiveId, mFileId);
            block.state = BUCKET_ACTIVE;

            /* add to LRU cache */
            mLRUCache->put(block.blockId, block.bucketId);
            /* update block info */
            mBlockArray[blockId] = block;
            /* mark loading log */
            mManifest->logLoadBlock(block);

        } else if (mBlockArray[blockId].state == BUCKET_USED ||
                   mBlockArray[blockId].state == BUCKET_ACTIVE) {
            /* activate the block */
            /* inactivate first if Current Quota is used up */
            if (getNumAcquiredBuckets() == getCurQuota()) {
                /* try to inactivate from activate list first. If no active block, then
                 * free a preAllocatedBucket to make sure we never exceed quota size */
                if (mLRUCache->size() > 0) {
                    std::vector<int> blockIds = mLRUCache->removeNumOfKeys(1);
                    std::vector<Block> blocksToInactivate;
                    for (int i : blockIds) {
                        blocksToInactivate.push_back(mBlockArray[i]);
                    }
                    std::vector<Block> turedToUsedBlocks =
                            mSharedMemoryContext->inactivateBuckets(blocksToInactivate,
                                                                    mFileId,
                                                                    mActiveId,
                                                                    mIsWrite);

                    /* update block status*/
                    for (Block b : turedToUsedBlocks) {
                        mBlockArray[b.blockId].state = b.state;

                        /* the ending block been inactivated, flush EoF */
                        if ((uint32_t)b.blockId + 1 == mBlockArray.size()){
                            /* update EoF from SharedMemory */
                            getSharedMemEof();
                            /* add the Eof log */
                            RecOpaque opaque;
                            opaque.updateEof.eof = mEof;
                            mManifest->logUpdateEof(opaque);
                        }
                    }
                    /* log inactivate buckets */
                    mManifest->logInactivateBucket(turedToUsedBlocks);
                } else {
                    /* free a bucket from preAllocatedBucket */
                    if (mPreAllocatedBuckets.size() == 0) {
                        THROW(GopherwoodInternalException,
                              "[ActiveStatus] PreAllocatedBuckets or LRUCacheBuckets should not be both 0!");
                    }

                    std::list<Block> freeOneList;
                    Block block = mPreAllocatedBuckets.back();
                    freeOneList.push_back(block);
                    mPreAllocatedBuckets.pop_back();

                    mSharedMemoryContext->releaseBuckets(freeOneList);
                    /* log release buckets */
                    mManifest->logReleaseBucket(freeOneList);
                }
            } else if (getNumAcquiredBuckets() > getCurQuota()) {
                THROW(GopherwoodInternalException,
                      "[ActiveStatus] Current quoto exceed max quota size!");
            }

            /* activate the block */
            rc = mSharedMemoryContext->activateBucket(mFileId,
                                                      mBlockArray[blockId],
                                                      mActiveId,
                                                      mIsWrite);
            /* the block is activated by me */
            if (rc == 1) {
                mManifest->logActivateBucket(mBlockArray[blockId]);
            } else if (rc == -1) {
                THROW(GopherwoodException, "[ActiveStatus] activateBucket in SharedMemory got error!");
            }

            mLRUCache->put(mBlockArray[blockId].blockId, mBlockArray[blockId].bucketId);
        } else {
            THROW(GopherwoodException, "[ActiveStatus] block active status mismatch!");
        }
    SHARED_MEM_END

    /* load the block */
    if (loadBlock) {
        BlockInfo info;
        info.fileId = mFileId;
        info.blockId = blockId;
        info.bucketId = mBlockArray[blockId].bucketId;
        info.isLocal = false;
        info.offset = InvalidBlockOffset;

        mOssWorker->readBlock(info);

        /* delete the remote block */
        BlockInfo deleteBlockinfo;
        deleteBlockinfo.bucketId = -1;
        deleteBlockinfo.blockId = blockId;
        deleteBlockinfo.fileId = mFileId;
        deleteBlockinfo.isLocal = false;
        mOssWorker->deleteBlock(deleteBlockinfo);

        SHARED_MEM_BEGIN
            mSharedMemoryContext->markLoadFinish(mBlockArray[blockId], mActiveId, mFileId);
            /* update statistics */
            mNumLoaded++;
        SHARED_MEM_END
    }
    /* if the block is loading by other process, wait until it finished */
    if (rc == 2) {
        bool stillLoading = true;

        while (true) {
            sleep_for(seconds(5));
            SHARED_MEM_BEGIN
                //stillLoading = mSharedMemoryContext->isBlockLoading(mBlockArray[blockId], mFileId);
            SHARED_MEM_END

            if (!stillLoading) {
                break;
            }
        }
    }
}

void ActiveStatus::activateBlockWithPreload(int blockId) {
    Block *theLoadingBlock = NULL;
    bool loadBlock = false;
    bool loadingByOther = false;
    int rc = -1;

    /* to eliminate the Shared Memory consistent issues,
     * We acquire new blocks first, then we activate the block.
     * Actually only loadBlock need acquire new block, but we
     * still acquire for all cases to simplify the logic. */
    if (mPreAllocatedBuckets.size() == 0) {
        acquireNewBlocks();
    }

    /* all blocks not activated by me can not be trusted
     * Need to lock Shared Memory and catch up logs */
    while (true) {
        SHARED_MEM_BEGIN
            if (!mBlockArray[blockId].isLocal) {
                bool markSuccess;

                /* build the block */
                theLoadingBlock = &mPreAllocatedBuckets.front();

                /* If the markBucketLoading failed, then it means the block is loading by others */
                markSuccess = mSharedMemoryContext->markBucketLoading(theLoadingBlock->bucketId, blockId, mActiveId,
                                                                      mFileId);
                if (markSuccess) {
                    mPreAllocatedBuckets.pop_front();
                    theLoadingBlock->blockId = blockId;
                    loadBlock = true;
                } else {
                    loadingByOther = true;
                }

            } else if (mBlockArray[blockId].state == BUCKET_USED ||
                       mBlockArray[blockId].state == BUCKET_ACTIVE) {
                /* activate the block */
                /* inactivate first if Current Quota is used up */
                if (getNumAcquiredBuckets() == getCurQuota()) {
                    /* try to inactivate from activate list first. If no active block, then
                     * free a preAllocatedBucket to make sure we never exceed quota size */
                    if (mLRUCache->size() > 0) {
                        std::vector<int> blockIds = mLRUCache->removeNumOfKeys(1);
                        std::vector<Block> blocksToInactivate;
                        for (int i : blockIds) {
                            blocksToInactivate.push_back(mBlockArray[i]);
                        }
                        std::vector<Block> turedToUsedBlocks =
                                mSharedMemoryContext->inactivateBuckets(blocksToInactivate,
                                                                        mFileId,
                                                                        mActiveId,
                                                                        mIsWrite);

                        /* update block status*/
                        for (Block b : turedToUsedBlocks) {
                            mBlockArray[b.blockId].state = b.state;

                            /* the ending block been inactivated, flush EoF */
                            if ((uint32_t)b.blockId + 1 == mBlockArray.size()){
                                /* update EoF from SharedMemory */
                                getSharedMemEof();
                                /* add the Eof log */
                                RecOpaque opaque;
                                opaque.updateEof.eof = mEof;
                                mManifest->logUpdateEof(opaque);
                            }
                        }
                        /* log inactivate buckets */
                        mManifest->logInactivateBucket(turedToUsedBlocks);
                    } else {
                        /* free a bucket from preAllocatedBucket */
                        if (mPreAllocatedBuckets.size() == 0) {
                            THROW(GopherwoodInternalException,
                                  "[ActiveStatus] PreAllocatedBuckets or LRUCacheBuckets should not be both 0!");
                        }

                        std::list<Block> freeOneList;
                        Block block = mPreAllocatedBuckets.back();
                        freeOneList.push_back(block);
                        mPreAllocatedBuckets.pop_back();

                        mSharedMemoryContext->releaseBuckets(freeOneList);
                        /* log release buckets */
                        mManifest->logReleaseBucket(freeOneList);
                    }
                } else if (getNumAcquiredBuckets() > getCurQuota()) {
                    THROW(GopherwoodInternalException,
                          "[ActiveStatus] Current quoto exceed max quota size!");
                }

                /* activate the block */
                rc = mSharedMemoryContext->activateBucket(mFileId,
                                                          mBlockArray[blockId],
                                                          mActiveId,
                                                          mIsWrite);
                /* the block is activated by me */
                if (rc == 1) {
                    mManifest->logActivateBucket(mBlockArray[blockId]);
                } else if (rc == -1) {
                    THROW(GopherwoodException, "[ActiveStatus] activateBucket in SharedMemory got error!");
                }

                mLRUCache->put(mBlockArray[blockId].blockId, mBlockArray[blockId].bucketId);
            } else {
                THROW(GopherwoodInternalException, "[ActiveStatus] block active status mismatch!");
            }
        SHARED_MEM_END

        if (loadBlock) {
            BlockInfo info;
            info.fileId = mFileId;
            info.blockId = blockId;
            info.bucketId = theLoadingBlock->bucketId;
            info.isLocal = false;
            info.offset = InvalidBlockOffset;

            thread loader;
            /* create a thread to load this block */
            CREATE_THREAD(loader, bind(&ActiveStatus::loadBlock, this, info));
            mLoadingBuckets.push_back(*theLoadingBlock);
        }

        /* if the block is loading by other process, wait until it finished and try to activate it again */
        if (loadingByOther) {
            bool stillLoading = true;

            while (true) {
                sleep_for(seconds(5));
                /* we don't need to check the Connection area in ShareMem again.
                 * If the load finished by others, when we get the ShareMem lock,
                 * the manifest log should have been replayed in my ActiveStatus
                 * TODO: There are still risk that the block been evicted again
                 * TODO: during my waiting time. The probability is so low that
                 * TODO: we can just lower down the wait time by now. */
                SHARED_MEM_BEGIN
                    if (mBlockArray[blockId].isLocal) {
                        stillLoading = false;
                    }
                SHARED_MEM_END

                if (!stillLoading) {
                    break;
                }
            }
        }

        /* the block been activated, break the loop */
        break;
    }
}

void ActiveStatus::loadBlock(BlockInfo info) {
}

void ActiveStatus::logEvictBlock(BlockInfo info) {
    Block block(InvalidBucketId, info.blockId, false, BUCKET_FREE);
    if(mFileId==info.fileId){
        mManifest->logEvcitBlock(block);
        mBlockArray[block.blockId] = block;
    }else {
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
}

/* NOTE: You should have acquired the ShareMem lock before calling me */
void ActiveStatus::getSharedMemEof(){
    /* Calculate the shared memory file EOF info */
    int numBlocks = mBlockArray.size();
    int32_t endBucketId = mBlockArray[numBlocks - 1].bucketId;
    int64_t lastDataSize = mSharedMemoryContext->getBucketDataSize(endBucketId, mFileId, numBlocks - 1);
    int64_t shareMemEof = (numBlocks - 1) * Configuration::LOCAL_BUCKET_SIZE + lastDataSize;

    /* update my Eof if others has write beyond me */
    if (shareMemEof > mEof) {
        mEof = shareMemEof;
    }
}

/* flush cached Manifest logs to disk */
void ActiveStatus::flush() {
    int64_t eofBeforeCatchUp = mEof;
    SHARED_MEM_BEGIN
        /* no other file exceed my Eof, I should try to flush Eof info */
        if (eofBeforeCatchUp <= mEof) {
            int numBlocks = mBlockArray.size();
            /* if the last block is not in activate status, then the Eof info should have been flushed */
            if (mBlockArray[numBlocks-1].isLocal && mBlockArray[numBlocks-1].state == BUCKET_ACTIVE) {
                /* update EoF from SharedMemory */
                getSharedMemEof();
                /* add the Eof log */
                RecOpaque opaque;
                opaque.updateEof.eof = mEof;
                mManifest->logUpdateEof(opaque);
            } else {
                LOG(DEBUG1, "[ActiveStatus]          |"
                        "flush: The ending block is inactivated, should have flushed the EoF log!");
            }
        } else {
            THROW(GopherwoodInternalException,
                  "[ActiveStatus::updateCurBlockSize] Unexpected File Eof");
        }
    SHARED_MEM_END
}

/* truncate existing Manifest file and flush latest block status to it */
void ActiveStatus::close(bool isCancel) {
    std::vector<Block> localBlocks;
    std::vector<Block> remoteBlocks;

    SHARED_MEM_BEGIN
        /* get blocks to inactivate */
        std::vector<int> activeBlockIds = mLRUCache->removeNumOfKeys(mLRUCache->size());
        std::vector<Block> activeBlocks;
        for (int32_t activeBlockId : activeBlockIds) {
            activeBlocks.push_back(mBlockArray[activeBlockId]);
        }

        /* release all preAllocatedBlocks & active buckets */
        if (mPreAllocatedBuckets.size() > 0) {
            mSharedMemoryContext->releaseBuckets(mPreAllocatedBuckets);
            /* log release buckets */
            mManifest->logReleaseBucket(mPreAllocatedBuckets);
        }

        /* inactivate buckets, in other words, remove the marker of current ActiveId */
        std::vector<Block> turedToUsedBlocks = mSharedMemoryContext->inactivateBuckets(activeBlocks,
                                                                                       mFileId,
                                                                                       mActiveId,
                                                                                       mIsWrite);
        /* update block status*/
        for (Block b : turedToUsedBlocks) {
            mBlockArray[b.blockId].state = b.state;

            /* the ending block been inactivated, flush EoF */
            if ((uint32_t)b.blockId + 1 == mBlockArray.size()){
                /* update EoF from SharedMemory */
                getSharedMemEof();
                /* add the Eof log */
                RecOpaque opaque;
                opaque.updateEof.eof = mEof;
                mManifest->logUpdateEof(opaque);
            }
        }

        if (turedToUsedBlocks.size() > 0) {
            /* log inactivate buckets */
            mManifest->logInactivateBucket(turedToUsedBlocks);
        }

        /* this will set the shouldDestroy field */
        unregistInSharedMem(isCancel);

        if (!mSharedMemoryContext->isFileOpening(mFileId)) {
            if (mShouldDestroy) {
                /* check all blocks are not in active status */
                for (Block block : mBlockArray) {
                    if (block.isLocal && block.state == BUCKET_USED) {
                        localBlocks.push_back(block);
                    } else if (block.isLocal && block.state == BUCKET_ACTIVE) {
                        THROW(GopherwoodException,
                              "[ActiveStatus] File %s still using active bucket %d",
                              mFileId.toString().c_str(), block.bucketId);
                    } else if (!block.isLocal) {
                        remoteBlocks.push_back(block);
                    } else {
                        THROW(GopherwoodException,
                              "[ActiveStatus] Dead Zone, Internal Error!");
                    }
                }
                /* delete the used blocks first */
                mSharedMemoryContext->deleteBlocks(localBlocks, mFileId);
                /* delete the Manifest File */
                mManifest->destroy();
            } else {
                /* truncate existing Manifest file and flush latest block status to it.
                 * NOTES: Only do the Manifest log shrinking if nobody is opening
                 * this file. */
                RecOpaque opaque;
                opaque.fullStatus.eof = mEof;
                mManifest->logFullStatus(mBlockArray, opaque);
            }
        } else {
            mShouldDestroy = false;
        }

        /* clear LRU & blockArray */
        mBlockArray.clear();
        mLRUCache.reset();

    SHARED_MEM_END

    if (mShouldDestroy) {
        /* remove all remote file */
        if (remoteBlocks.size() > 0) {
            for (Block remoteBlock : remoteBlocks) {
                BlockInfo info;
                info.bucketId = remoteBlock.bucketId;
                info.blockId = remoteBlock.blockId;
                info.fileId = mFileId;
                info.isLocal = remoteBlock.isLocal;

                mOssWorker->deleteBlock(info);
            }
        }
    }
}

void ActiveStatus::catchUpManifestLogs() {
    std::vector<Block> blocks;

    while (true) {
        RecordHeader header = mManifest->fetchOneLogRecord(blocks);
        if (header.type == RecordType::invalidLog) {
            break;
        }
        /* integrity checks */
        assert(header.numBlocks == blocks.size());

        /* replay the log */
        switch (header.type) {
            case RecordType::activeBlock:
                LOG(DEBUG1, "[ActiveStatus]          |"
                        "Replay activeBlock log record with %lu blocks.", blocks.size());
                for (Block block : blocks) {
                    mBlockArray[block.blockId].state = BUCKET_ACTIVE;
                }
                break;
            case RecordType::inactiveBlock:
                LOG(DEBUG1, "[ActiveStatus]          |"
                        "Replay inactiveBlock log record with %lu blocks.", blocks.size());
                for (Block block : blocks) {
                    mBlockArray[block.blockId].state = BUCKET_USED;
                }
                break;
            case RecordType::acquireNewBlock:
                /* No need to replay this log for Read ActiveStatus and Delete ActiveStatus,
                 * the preAllocatedBuckets should bundled with Write ActiveStatus
                 * Since we only support one Write ActiveStatus at a time:
                 * 1. Read/Delete ActiveStatus do not need the preAllocatedBuckets info
                 * 2. When Write ActiveStatus do the log replay work, all preAllocatedBuckets should have
                 *    been released
                 * TODO: The only case to replay this log is to check the log integrity */
                LOG(DEBUG1, "[ActiveStatus]          |"
                        "Skip acquireNewBlock log record with %lu blocks.", blocks.size());
                break;
            case RecordType::releaseBlock:
                /* As described in acquireNewBlock log, the only need to replay this
                 * log is to check the log integrity */
                LOG(DEBUG1, "[ActiveStatus]          |"
                        "Skip releaseBlock log record with %lu blocks.", blocks.size());
                break;
            case RecordType::extendBlock:
                LOG(DEBUG1, "[ActiveStatus]          |"
                        "Replay assignBlock log record with %lu blocks.", blocks.size());
                assert(header.numBlocks == 1);
                mBlockArray.push_back(blocks[0]);
                mEof = header.opaque.extendBlock.eof;
                break;
            case RecordType::evictBlock:
                LOG(DEBUG1, "[ActiveStatus]          |"
                        "Replay evictBlock log record with %lu blocks.", blocks.size());
                assert(header.numBlocks == 1);
                if (mBlockArray[blocks[0].blockId].isLocal && mBlockArray[blocks[0].blockId].state == BUCKET_USED) {
                    mBlockArray[blocks[0].blockId] = blocks[0];
                } else {
                    THROW(GopherwoodException,
                          "[ActiveStatus] The block %d status is not BUCKET_USED when replaying the evictBlock log ",
                          blocks[0].blockId);
                }
                break;
            case RecordType::loadBlock:
                LOG(DEBUG1, "[ActiveStatus]          |"
                        "Replay loadBlock log record with %lu blocks.", blocks.size());
                assert(header.numBlocks == 1);
                if (!mBlockArray[blocks[0].blockId].isLocal) {
                    mBlockArray[blocks[0].blockId] = blocks[0];
                } else {
                    THROW(GopherwoodException,
                          "[ActiveStatus] The block %d is not remote when replaying the loadBlock log ",
                          blocks[0].blockId);
                }
                break;
            case RecordType::fullStatus:
                LOG(DEBUG1, "[ActiveStatus]          |"
                        "Replay fullStatus log record with %lu blocks. EOF=%lu", blocks.size(),
                    header.opaque.fullStatus.eof);
                for (Block block : blocks) {
                    mBlockArray.push_back(block);
                }
                mEof = header.opaque.fullStatus.eof;
                break;
            case RecordType::updateEof:
                assert(header.numBlocks == 0);
                LOG(DEBUG1, "[ActiveStatus]          |"
                        "Replay updateEof log record eof=%ld.", header.opaque.updateEof.eof);
                mEof = header.opaque.updateEof.eof;
                break;
            default:
                THROW(GopherwoodNotImplException,
                      "[ActiveStatus] Log type %d not implemented when catching up logs.",
                      header.type);
        }
        blocks.clear();
    }
}

ActiveStatus::~ActiveStatus() {
}

}
}