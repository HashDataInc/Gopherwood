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
#include "core/ActiveStatus.h"

namespace Gopherwood {
namespace Internal {

/* [IMPORTANT] Usually you need to acquire Shared Memory lock
 * before acquire Manifest Lock */
#define MANIFEST_LOG_BEGIN  mManifest->lock();
#define MANIFEST_LOG_END    mManifest->unlock();

#define SHARED_MEM_BEGIN    mSharedMemoryContext->lock();
#define SHARED_MEM_END      mSharedMemoryContext->unlock();

ActiveStatus::ActiveStatus(FileId fileId,
                           shared_ptr<SharedMemoryContext> sharedMemoryContext,
                           bool isCreate,
                           bool isWrite) :
        mFileId(fileId),
        mSharedMemoryContext(sharedMemoryContext),
        mIsWrite(isWrite){
    registInSharedMem();
    mNumBlocks = 0;
    mPos = 0;
    mEof = 0;
    mBlockSize = Configuration::LOCAL_BLOCK_SIZE;

    /* check file exist if not creating a new file */
    std::string manifestFileName= getManifestFileName(mFileId);
    if (!isCreate && access( manifestFileName.c_str(), F_OK ) == -1){
        THROW(GopherwoodInvalidParmException,
              "[ActiveStatus::ActiveStatus] File does not exist %s",
              manifestFileName.c_str());
    }
    mManifest = shared_ptr<Manifest>(new Manifest(manifestFileName));
    mLRUCache = shared_ptr<LRUCache<int, Block>>(new LRUCache<int, Block>(Configuration::CUR_QUOTA_SIZE));
    /* catch up logs */
    catchUpManifestLogs();
}

/* Shared Memroy activeStatus field will maintain all connected files */
void ActiveStatus::registInSharedMem(){
    SHARED_MEM_BEGIN
    mActiveId = mSharedMemoryContext->regist(getpid(), mFileId);
    if (mActiveId == -1){
        THROW(GopherwoodSharedMemException,
              "[ActiveStatus::registInSharedMem] Exceed max connection limitation %d",
              mSharedMemoryContext->getNumMaxActiveStatus());
    }
    LOG(INFO, "[ActiveStatus] Registered successfully, ActiveID=%d, PID=%d", mActiveId, getpid());
    SHARED_MEM_END
}

void ActiveStatus::unregistInSharedMem() {
    if(mActiveId == -1)
        return;

    int rc = mSharedMemoryContext->unregist(mActiveId, getpid());
    if (rc != 0){
        mSharedMemoryContext->unlock();
        THROW(GopherwoodSharedMemException,
              "[ActiveStatus::unregistInSharedMem] connection info mismatch with SharedMem ActiveId=%d, PID=%d",
              mActiveId, getpid());
    }
    LOG(INFO, "[ActiveStatus] Unregistered successfully, ActiveID=%d, PID=%d", mActiveId,getpid());
    mActiveId = -1;
}

int64_t ActiveStatus::getPosition() {
    return mPos;
}

void ActiveStatus::setPosition(int64_t pos) {
    mPos = pos;
    if (mPos > mEof) {
        mEof = mPos;
    }
}

int64_t ActiveStatus::getEof() {
    return mEof;
}

Block ActiveStatus::getCurBlock() {
    return mBlockArray[mPos / mBlockSize];
}

int64_t ActiveStatus::getCurBlockOffset() {
    return mPos % mBlockSize;
}

std::string ActiveStatus::getManifestFileName(FileId fileId) {
    std::stringstream ss;
    ss << mSharedMemoryContext->getWorkDir() << Configuration::MANIFEST_FOLDER << '/' << mFileId.hashcode << '-'
       << mFileId.collisionId;
    return ss.str();
}


/* [IMPORTANT] This is the main entry point of adjusting active status. OutpuStream/InputStream
 * will call this function to write/read to multi blocks. When mPos reaches block not activated
 * by current file instance, ActiveStatus will adjust the block status.*/
BlockInfo ActiveStatus::getCurBlockInfo() {
    int curBlockIndex = mPos / mBlockSize;

    /* adjust the active block status */
    adjustActiveBlock(curBlockIndex);

    /* build the block info */
    BlockInfo info;
    Block block = getCurBlock();
    info.fileId = mFileId;
    info.blockId = block.bucketId;
    info.isLocal = block.isLocal;
    info.offset = getCurBlockOffset();
    return info;
}

void ActiveStatus::adjustActiveBlock(int curBlockInd) {
    if (curBlockInd + 1 > mNumBlocks) {
        extendOneBlock();
    }
    /* need to mark the block to my active block */
    else if (!mBlockArray[curBlockInd].isMyActive){

        if(!mBlockArray[curBlockInd].isLocal) {

        }else if(mBlockArray[curBlockInd].state == BUCKET_USED) {
            activateBlock(curBlockInd);
        }else if(mBlockArray[curBlockInd].state == BUCKET_ACTIVE &&
                 !mBlockArray[curBlockInd].isMyActive){
            activateBlock(curBlockInd);
        }

    }
}

/* All block activation should follow these steps:
 * 1. Check shared memory for the current quota
 * 2(a). If still have quota available, and have 0 or 2 available
 *       Then -> acquire more buckets for preAllocatedBlocks
 * 2(b). If still have quota available, and no 0 or 2 available
 *       Then -> play with current owned buckets
 * 3(a). If quota equal to current active block num, and have 0 or 2 available
 *       Then -> inactivate blocks from LRU first, then acquire new blocks
 * 3(b). If quota equal to current active block num, and have 0 or 2 available
 *       Then -> play with current owned buckets
 * 4. If quota smaller than current active block num,
 *       Then -> release blocks and use own quota
 * Notes: When got chance to acquire new blocks, active status will try to
 *        pre acquire a number of buckets to reduce the Shared Memory contention. */
void ActiveStatus::acquireNewBlocks() {
    std::vector<Block> blocksForLog;
    std::vector<int32_t> newBlocks;
    std::vector<int32_t> evictBuckets;

    int32_t numToAcquire = 0;
    int32_t numToInactivate = 0;

    SHARED_MEM_BEGIN
    uint32_t quota = mSharedMemoryContext->calcDynamicQuotaNum();
    int numFreeBuckets = mSharedMemoryContext->getFreeBucketNum();
    int numUsedBuckets = mSharedMemoryContext->getUsedBucketNum();
    int numAvailable = numFreeBuckets + numUsedBuckets;
    LOG(INFO, "[ActiveStatus] current quota is %u, num availables is %d.",
        quota, numAvailable);

    /************************************************
     *      Determin the acquire policy first       *
     ************************************************/
    if (mLRUCache->size() < quota) {
        /* 2(a) acquire more buckets for preAllocatedBlocks */
        if (numAvailable > 0) {
            numToAcquire = numAvailable > Configuration::PRE_ALLOCATE_BUCKET_NUM ?
                           Configuration::PRE_ALLOCATE_BUCKET_NUM : numAvailable;
        }
            /* 2(b) play with current owned buckets */
        else {
            /* TODO: Not implemented */
        }
    } else if (mLRUCache->size() == quota) {
        /* 3(a) inactivate blocks from LRU first, then acquire new blocks */
        if (numAvailable > 0) {
            /* TODO: Not implemented */
        }
            /* 3(b) play with current owned buckets */
        else {
            /* TODO: Not implemented */
        }
    }
        /* 4 release blocks and use own quota*/
    else {
        /* TODO: Not implemented */
    }

    /************************************************
     *      Real Operations on SharedMemory         *
     ************************************************/
    /* release first */
    if (numToInactivate > 0){

    }

    /* acquire new buckets */
    if (numToAcquire > 0){
        int numAcqurieFree;
        int numToEvict;
        if (numToAcquire > numFreeBuckets) {
            numAcqurieFree = numFreeBuckets;
            numToEvict = numToAcquire - numAcqurieFree;
        } else {
            numAcqurieFree = numToAcquire;
            numToEvict = 0;
        }

        newBlocks = mSharedMemoryContext->acquireFreeBucket(mActiveId, numAcqurieFree, mFileId, mIsWrite);
        evictBuckets = mSharedMemoryContext->markBucketEvicting(mActiveId, numToEvict);
    }
    SHARED_MEM_END

    /* add free buckets to preAllocatedBlocks */
    for (std::vector<int32_t>::size_type i = 0; i < newBlocks.size(); i++) {
        LOG(INFO, "[ActiveStatus] add block %d to pre-allocated bucket array.", newBlocks[i]);
        Block newBlock(newBlocks[i],
                       InvalidBlockId,
                       LocalBlock,
                       BUCKET_ACTIVE,
                       true);/*is my active block*/
        blocksForLog.push_back(newBlock);
        mPreAllocatedBuckets.push_back(newBlock);
    }

    /* add evict buckets to preAllocatedBlocks */
    for (uint32_t i=0; i<evictBuckets.size(); i++){
        int bucketId = evictBuckets[i];
        /* Mark the evict/load status of current ActiveStatus in SharedMem  */
        SHARED_MEM_BEGIN
        BlockInfo blockInfo = mSharedMemoryContext->evictBucketStart(bucketId, mActiveId);
        SHARED_MEM_END
        LOG(INFO, "[ActiveStatus] Start evicting File %s, blockId %d",
            blockInfo.fileId.toString().c_str(), blockInfo.blockId);

        /* evict the block */


        /* Set block evict finished and and activate the block */
        SHARED_MEM_BEGIN
        mSharedMemoryContext->evictBucketFinish(bucketId, mActiveId, mFileId, mIsWrite);
        Block newBlock(bucketId, InvalidBlockId, LocalBlock, BUCKET_ACTIVE, true);
        LOG(INFO, "[ActiveStatus] add block %d to pre-allocated bucket array.", newBlocks[i]);
        blocksForLog.push_back(newBlock);
        mPreAllocatedBuckets.push_back(newBlock);
        SHARED_MEM_END
    }

    /* Manifest Log */
    MANIFEST_LOG_BEGIN
    mManifest->logAcquireNewBlock(blocksForLog);
    MANIFEST_LOG_END
}

/* Extend the file to create a new block, the block will get bucket from the
 * pre allocated bucket array */
void ActiveStatus::extendOneBlock() {
    std::vector<Block> blocksModified;

    if (mPreAllocatedBuckets.size() == 0) {
        acquireNewBlocks();
    }

    /* build the block */
    Block b = mPreAllocatedBuckets.back();
    mPreAllocatedBuckets.pop_back();
    b.blockId = mNumBlocks;

    /* add to block array */
    mBlockArray.push_back(b);
    mNumBlocks++;

    /* add to LRU cache */
    mLRUCache->put(b.blockId, b);

    /* prepare for log */
    blocksModified.push_back(b);

    SHARED_MEM_BEGIN
    /* update file info to shared memory */
    mSharedMemoryContext->updateActiveFileInfo(blocksModified, mFileId);
    /* Manifest Log */
    MANIFEST_LOG_BEGIN
    RecOpaque opaque;
    opaque.extendBlock.eof = mEof;
    mManifest->logExtendBlock(blocksModified, opaque);
    MANIFEST_LOG_END
    SHARED_MEM_END
}

/* activate a block if it's not been marked */
void ActiveStatus::activateBlock(int blockInd){
    SHARED_MEM_BEGIN
    /* TODO: catch up log inside shared mem */

    /* activate the block */
    bool activated = mSharedMemoryContext->activateBucket(mFileId, mBlockArray[blockInd], mActiveId, mIsWrite);
    mLRUCache->put(mBlockArray[blockInd].blockId, mBlockArray[blockInd]);

    /* the block is activated by me */
    if (activated){
        //mManifest-> TODO: log inactivate blocks
    }
    SHARED_MEM_END
}

/* flush cached Manifest logs to disk
 * TODO: Currently all log record are flushed immediately, we just add UpdateEof log */
void ActiveStatus::flush() {
    MANIFEST_LOG_BEGIN
    RecOpaque opaque;
    opaque.updateEof.eof = mEof;
    mManifest->logUpdateEof(opaque);
    MANIFEST_LOG_END
}

/* truncate existing Manifest file and flush latest block status to it */
void ActiveStatus::close() {
    SHARED_MEM_BEGIN
    MANIFEST_LOG_BEGIN

    /* get blocks to inactivate */
    std::vector<int> activeBlockIds = mLRUCache->getAllKeyObject();
    std::vector<Block> activeBlocks;
    for (uint32_t i = 0; i < activeBlockIds.size(); i++) {
        activeBlocks.push_back(mBlockArray[activeBlockIds[i]]);
    }

    /* release all preAllocatedBlocks & active buckets */
    mSharedMemoryContext->releaseBuckets(mPreAllocatedBuckets);
    std::vector<Block> turedToUsedBlocks = mSharedMemoryContext->inactivateBuckets(activeBlocks, mFileId, mActiveId, mIsWrite);

    /* TODO: log inactivate blocks */
    //mManifest->log

    unregistInSharedMem();

    if (mSharedMemoryContext->isLastActiveStatusOfFile(mFileId)){
        /* truncate existing Manifest file and flush latest block status to it */
        RecOpaque opaque;
        opaque.fullStatus.eof = mEof;
        mManifest->logFullStatus(mBlockArray, opaque);
    }

    /* clear LRU & blockArray */
    mBlockArray.clear();
    /* TODO: clear mLRUCache */

    MANIFEST_LOG_END
    SHARED_MEM_END
}

/* delete the existing Manifest file and release the bucket of the file */
void ActiveStatus::remove(){
    SHARED_MEM_BEGIN
    MANIFEST_LOG_BEGIN

    /* release all preAllocatedBlocks & active buckets */
    mSharedMemoryContext->releaseBuckets(mPreAllocatedBuckets);

    /* release the used buckets*/
    mSharedMemoryContext->deleteBuckets(mBlockArray);

    unregistInSharedMem();

    /* delete the mainfest file*/
    mManifest->remove();

    /* clear LRU & blockArray */
    mBlockArray.clear();
    /* TODO: clear mLRUCache */

    MANIFEST_LOG_END
    SHARED_MEM_END
}

void ActiveStatus::catchUpManifestLogs() {
    std::vector<Block> blocks;

    MANIFEST_LOG_BEGIN
    while(true){
        RecordHeader header = mManifest->fetchOneLogRecord(blocks);
        if (header.type == RecordType::invalidLog){
            break;
        }
        /* integrety checks */
        assert(header.numBlocks == blocks.size());

        /* replay the log */
        switch (header.type) {
            case RecordType::inactiveBlock:
                LOG(INFO, "[ActiveStatus] got inactiveBlock log record with %lu blocks.", blocks.size());
                break;
            case RecordType::acquireNewBlock:
                if (mIsWrite){
                    LOG(INFO, "[ActiveStatus] got acquireNewBlock log record with %lu blocks.", blocks.size());
                    /* TODO: Apply this log*/
                }
                break;
            case RecordType::extendBlock:
                LOG(INFO, "[ActiveStatus] got assignBlock log record with %lu blocks.", blocks.size());
                assert(header.numBlocks == 1);
                mBlockArray.push_back(blocks[0]);
                mNumBlocks++;
                mEof = header.opaque.extendBlock.eof;
                break;
            case RecordType::fullStatus:
                LOG(INFO, "[ActiveStatus] got fullStatus log record with %lu blocks. EOF=%lu", blocks.size(), header.opaque.fullStatus.eof);
                for(uint32_t i=0; i<blocks.size(); i++){
                    mBlockArray.push_back(blocks[i]);
                    mNumBlocks++;
                }
                mEof = header.opaque.fullStatus.eof;
                break;
            case RecordType::updateEof:
                assert(header.numBlocks == 0);
                mEof = header.opaque.updateEof.eof;
                break;
            default:
                THROW(GopherwoodNotImplException,
                      "[ActiveStatus] Log type %d not implemented when catching up logs.",
                      header.type);
        }
        blocks.clear();
    }
    MANIFEST_LOG_END
}

ActiveStatus::~ActiveStatus() {
}

}
}