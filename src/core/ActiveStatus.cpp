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
#include "common/Logger.h"
#include "core/ActiveStatus.h"

namespace Gopherwood {
namespace Internal {

ActiveStatus::ActiveStatus(FileId fileId, shared_ptr<SharedMemoryContext> sharedMemoryContext) :
        mSharedMemoryContext(sharedMemoryContext){
    mNumBlocks = 0;
    mPos = 0;
    mEof = 0;
    mfileId.hashcode = fileId.hashcode;
    mfileId.collisionId = fileId.collisionId;
}

int64_t ActiveStatus::getPosition(){
    return mPos;
}

void ActiveStatus::setPosition(int64_t pos){
    mPos = pos;
    if (mPos > mEof){
        mEof = mPos;
    }
}

/* When calling this function, it means out/in stream
 * wants to access this block. Thus we need to update
 * quota or check the LRU status. */
BlockInfo ActiveStatus::getCurBlockInfo()
{
    /* check if we need to acquire more blocks */
    if (needNewBlock()) {
        acquireNewBlocks();
    }

    /* build the block info */
    BlockInfo info;
    Block block = getCurBlock();
    info.id = block.bucketId;
    info.isLocal = block.isLocal;
    info.offset = getCurBlockOffset();
    return info;
}
Block ActiveStatus::getCurBlock()
{
    return mBlockArray[mPos/Configuration::LOCAL_BLOCK_SIZE];
}

int64_t ActiveStatus::getCurBlockOffset()
{
    return mPos % Configuration::LOCAL_BLOCK_SIZE;
}

bool ActiveStatus::needNewBlock()
{
    int curBlockInd = mPos/Configuration::LOCAL_BLOCK_SIZE;
    return (mNumBlocks <= 0 ||                              // empty file
            curBlockInd >= mNumBlocks ||                    // append more data
            !mBlockArray[curBlockInd].isLocal ||            // block been evicted
            (mBlockArray[curBlockInd].isLocal &&            // block in used(2) state
             mBlockArray[curBlockInd].state==BUCKET_USED));
}

void ActiveStatus::acquireNewBlocks()
{
    std::vector<int32_t> newBlocks = mSharedMemoryContext->acquireBlock(mfileId);

    /* TODO:
     * put to pre allocated list first */

    for (std::vector<int32_t>::size_type i=0; i<newBlocks.size(); i++)
    {
        LOG(INFO, "[ActiveStatus::acquireNewBlocks] add block %d.", newBlocks[i]);
        Block newBlock(newBlocks[i], mBlockArray.size(),true, BUCKET_ACTIVE);
        mBlockArray.push_back(newBlock);
    }
}

ActiveStatus::~ActiveStatus() {

}

}
}