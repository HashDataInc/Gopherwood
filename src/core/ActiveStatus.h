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
#include "common/LRUCache.cpp"
#include "core/SharedMemoryContext.h"
#include "core/BlockStatus.h"
#include "core/Manifest.h"

namespace Gopherwood {
namespace Internal {

/**
 * ActiveStatus
 *
 * @desc An ActiveStatus instance maintains file block status. Three main
 * function of this class are:
 * 1. Communicate with Shared Memory to acquire/release local buckets
 * 2. Maintain Manifest Log to syncronize file status with Shared Memory status
 * 3. Provide the file/block status to OutputStream/InputStream
 * @BlockArray The array to save all block status
 * @PreAllocatedBlocks To eliminate the Share Memory contention issue, each time
 * it need to acquire new buckets, a number of buckets will be pre-allocated.
 * @SharedMemoryContext The filesystem level Shared Memory instance to control
 * bucket operations.
 */
class ActiveStatus {
public:
    ActiveStatus(FileId fileId,
                 shared_ptr<SharedMemoryContext> sharedMemoryContext,
                 bool isCreate,
                 bool isWrite
    );

    /*********** Getter and setters ***********/
    int64_t     getEof();
    int64_t     getPosition();
    void        setPosition(int64_t pos);

    /**** The main entry point to adjust block status ****/
    BlockInfo   getCurBlockInfo();

    void flush();
    void close();
    void remove();

    ~ActiveStatus();

private:
    void registInSharedMem();
    void unregistInSharedMem();

    Block       getCurBlock();
    int64_t     getCurBlockOffset();
    std::string getManifestFileName(FileId fileId);

    /***** active status block manipulations *****/
    void catchUpManifestLogs();
    void adjustActiveBlock(int curBlockInd);
    void acquireNewBlocks();
    void extendOneBlock();
    void activateBlock(int blockInd);

    /****************** Fields *******************/
    FileId mFileId;
    int32_t mActiveId;
    shared_ptr<SharedMemoryContext> mSharedMemoryContext;
    shared_ptr<Manifest> mManifest;
    shared_ptr<LRUCache<int, Block>> mLRUCache;

    bool mIsWrite;
    int64_t mPos;
    int64_t mEof;
    int32_t mNumBlocks;
    int64_t mBlockSize;

    std::vector<Block> mBlockArray;
    std::vector<Block> mPreAllocatedBuckets;
};


}
}

#endif //_GOPHERWOOD_CORE_ACTIVESTATUS_H_
