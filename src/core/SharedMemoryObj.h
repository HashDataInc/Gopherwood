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
#ifndef GOPHERWOOD_CORE_SHAREDMEMORYOBJ_H
#define GOPHERWOOD_CORE_SHAREDMEMORYOBJ_H

#include "platform.h"
#include "common/Memory.h"
#include "core/BlockStatus.h"
#include "file/FileId.h"

namespace Gopherwood {
namespace Internal {

#define BucketTypeMask 0xFFFFFFFC
#define BUCKET_FREE      0
#define BUCKET_ACTIVE    1
#define BUCKET_USED      2

#define InvalidPid -1
#define InvalidActiveId -1

typedef struct ShareMemHeader {
    uint8_t flags;
    char padding[3];

    /* num buckets */
    int32_t numBuckets;
    /* num max ActiveStatus instances */
    int32_t numMaxActiveStatus;

    /* Bucket Statistics */
    int32_t numFreeBuckets;
    int32_t numActiveBuckets;
    int32_t numUsedBuckets;
    int32_t numEvictingBuckets;

    void enter();

    void exit();

    void reset(int32_t totalBucketNum, int32_t maxConn) {
        flags = 0;
        numBuckets = totalBucketNum;
        numMaxActiveStatus = maxConn;
        numFreeBuckets = totalBucketNum;
        numActiveBuckets = 0;
        numUsedBuckets = 0;
        numEvictingBuckets = 0;
    };
} ShareMemHeader;

/* Bit usages in flags field (low to high)
 * bit 0~1:     Bucket type 0/1/2
 * bit 31:      Evicting bucket will set this bit to 1
 * */
#define SMBUCKET_MAX_CONCURRENT_OPEN 10
typedef struct ShareMemBucket {
    uint32_t flags;
    FileId fileId;
    int32_t fileBlockIndex;
    int32_t writeActiveId;
    int32_t evictActiveId;
    int32_t readActives[SMBUCKET_MAX_CONCURRENT_OPEN];

    /* getter/setter */
    bool isFreeBucket() { return (flags & 0x00000003) == 0 ? true : false; };
    bool isActiveBucket() { return (flags & 0x00000003) == 1 ? true : false; };
    bool isUsedBucket() { return (flags & 0x00000003) == 2 ? true : false; };
    bool isEvictingBucket() {return (flags & 0x80000000);};
    void setBucketFree() { flags = (flags & BucketTypeMask) | 0x00000000;};
    void setBucketActive() {flags = (flags & BucketTypeMask) | 0x00000001; };
    void setBucketUsed() { flags = (flags & BucketTypeMask) | 0x00000002; };
    void setBucketEvicting() { flags = (flags | 0x80000000);};
    void setBucketEvictFinish() { flags = (flags & 0x7FFFFFFF);};

    void reset();
    void markWrite(int activeId);
    void markRead(int activeId);
    void unmarkWrite(int activeId);
    void unmarkRead(int activeId);
    bool noActiveReadWrite();
} ShareMemBucket;

/* This field is to support multiple-read and protect single-wirte
 * Each ActiveStatus will regist here when constructing, and set the
 * evicting/loading status to let others know the overall status.
 * Operations are:
 * 1. Check whether all ActiveStatus of a File is closed
 * 2. Check the evicting ActiveStatus pid
 * 3. Check the loading status(from OSS) of a file block */
typedef struct ShareMemActiveStatus {
    int pid;
    int32_t flags;
    FileId fileId;
    int32_t fileBlockIndex;

    void setEvicting() { flags |= 0x00000001; };
    void setReading() { flags |= 0x00000002; };
    void setDeleting() {flags |= 0x00000004;};
    void unsetEvicting() { flags &= 0xFFFFFFFE; };
    void unsetReading() { flags |= 0xFFFFFFFD; };
    void unsetDeleting(){flags &= 0xFFFFFFFB;};

    void reset() {
        pid = InvalidPid;
        flags=0;
        fileId.reset();
        fileBlockIndex = InvalidBlockId;
    };
} ShareMemActiveStatus;

}
}
#endif //GOPHERWOOD_CORE_SHAREDMEMORYOBJ_H
