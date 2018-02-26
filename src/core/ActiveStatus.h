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
#ifndef _GOPHERWOOD_CORE_ACTIVESTATUS_H_
#define _GOPHERWOOD_CORE_ACTIVESTATUS_H_

#include "platform.h"

#include "file/FileId.h"
#include "core/SharedMemoryContext.h"

namespace Gopherwood {
namespace Internal {

typedef struct Block {
    int32_t id;

    Block(int32_t theId) : id(theId){};
} Block;

typedef struct BlockInfo {
    int32_t id;
    int64_t offset;
} BlockInfo;

class ActiveStatus {
public:
    ActiveStatus(FileId fileId, shared_ptr<SharedMemoryContext> sharedMemoryContext);

    int64_t getCurPosition() {
        return mPos;
    };

    BlockInfo getCurBlockInfo();

    ~ActiveStatus();
private:
    int32_t getCurBlockId();

    int64_t getCurBlockOffset();

    bool needNewBlock();

    void acquireNewBlocks();

    shared_ptr<SharedMemoryContext> mSharedMemoryContext;
    FileId mfileId;
    int64_t mPos;
    int32_t mNumBlocks;
    std::vector<Block> mBlockArray;
};


}
}

#endif //_GOPHERWOOD_CORE_ACTIVESTATUS_H_
