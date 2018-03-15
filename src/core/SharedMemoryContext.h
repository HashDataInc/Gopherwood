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
#ifndef _GOPHERWOOD_CORE_SHAREDMEMORYCONTEXT_H_
#define _GOPHERWOOD_CORE_SHAREDMEMORYCONTEXT_H_

#include "platform.h"
#include "common/Memory.h"
#include "core/SharedMemoryObj.h"

#include <boost/interprocess/mapped_region.hpp>

namespace Gopherwood {
namespace Internal {

using namespace boost::interprocess;

class SharedMemoryContext {
public:
    SharedMemoryContext(std::string dir, shared_ptr<mapped_region> region, int lockFD, bool reset);

    int regist(int pid, FileId fileId);
    int unregist(int activeId, int pid);

    int calcDynamicQuotaNum();
    bool isLastActiveStatusOfFile(FileId fileId);

    std::vector<int32_t> acquireFreeBlock(int activeId, int num, FileId fileId, bool isWrite);
    void releaseBlocks(std::vector<Block> &blocks);
    bool activateBlock(FileId fileId, Block& block, int activeId, bool isWrite);
    std::vector<Block> inactivateBlocks(std::vector<Block> &blocks, FileId fileId, int activeId, bool isWrite);
    void updateActiveFileInfo(std::vector<Block> &blocks, FileId fileId);

    /* evict logic related APIs*/
    std::vector<int32_t> markEvicting(int activeId, int num);
    ShareMemBucket* evictBlockStart(int32_t bucketId, int activeId);
    void evictBlockFinish(int32_t bucketId, int activeId, FileId fileId, int isWrite);


    void reset();
    void lock();
    void unlock();

    int32_t getFreeBucketNum();
    int32_t getActiveBucketNum();
    int32_t getUsedBucketNum();
    int32_t getEvictingBucketNum();

    std::string &getWorkDir();
    int32_t getNumMaxActiveStatus();

    ~SharedMemoryContext();

private:
    void printStatistics();

    std::string workDir;
    shared_ptr<mapped_region> mShareMem;
    int mLockFD;
    ShareMemHeader *header;
    ShareMemBucket *buckets;
    ShareMemActiveStatus *activeStatus;
};

}
}

#endif //_GOPHERWOOD_CORE_SHAREDMEMORYCONTEXT_H_
