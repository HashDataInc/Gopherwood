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
#include "core/ActiveStatus.h"

namespace Gopherwood {
namespace Internal {

ActiveStatus::ActiveStatus(FileId fileId, shared_ptr<SharedMemoryContext> sharedMemoryContext) :
        mSharedMemoryContext(sharedMemoryContext){
    mNumBlocks = 0;
    mfileId.hashcode = fileId.hashcode;
    mfileId.collisionId = fileId.collisionId;
}

BlockInfo ActiveStatus::getCurBlockInfo()
{
    if (needNewBlock())
    {
        acquireNewBlocks();
    }

    BlockInfo info;
    info.id = getCurBlockId();
    info.offset = getCurBlockOffset();
    return info;
}
int32_t ActiveStatus::getCurBlockId()
{
    return mBlockArray[mPos/Configuration::LOCAL_BLOCK_SIZE].id;
}

int64_t ActiveStatus::getCurBlockOffset()
{
    return mPos % Configuration::LOCAL_BLOCK_SIZE;
}

bool ActiveStatus::needNewBlock()
{
    return (mNumBlocks <= 0 || mPos/Configuration::LOCAL_BLOCK_SIZE >= mNumBlocks);
}

void ActiveStatus::acquireNewBlocks()
{
    std::vector<int32_t> newBlockIds = mSharedMemoryContext->acquireBlock(mfileId);

    for (std::vector<int32_t>::size_type i=0; i<newBlockIds.size(); i++)
    {
        Block newBlock(newBlockIds[i]);
        mBlockArray.push_back(newBlock);
    }
}

}
}