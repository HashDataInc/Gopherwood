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
    int16_t numMaxActiveStatus;
    /* Clock sweep hand: index of next bucket to consider grabbing */
    int32_t nextVictimBucket;

    /* Bucket Statistics */
    uint32_t numFreeBuckets;
    uint32_t numActiveBuckets;
    uint32_t numUsedBuckets;
    uint32_t numEvictingBuckets;

    void enter();

    void exit();

    void reset(int32_t totalBucketNum, uint16_t maxConn) {
        flags = 0;
        numBuckets = totalBucketNum;
        numMaxActiveStatus = maxConn;
        nextVictimBucket = 0;
        numFreeBuckets = totalBucketNum;
        numActiveBuckets = 0;
        numUsedBuckets = 0;
        numEvictingBuckets = 0;
    };
} ShareMemHeader;

/* used in ShareMemBucket to remember ActiveStatus
 * that are using this bucket*/
typedef struct BucketActiveInfo {
    /* 0-free, 1-write, 2-read */
    uint16_t flags;
    int16_t activeId;

    void reset() { flags = 0; activeId = InvalidActiveId; };
    void setWrite() {flags = 1;};
    void setRead() {flags = 2;};
} BucketActiveInfo;

/* Max ActiveStatus number that can mark activate on a bucket
 * NOTE: max setting is the max_value of int16_t  */
#define SMBUCKET_MAX_CONCURRENT_OPEN 32

/* Bit usages in flags field (low to high)
 * bit 0~1:     Bucket type 0/1/2
 * bit 29:      Mark the block is loading
 * bit 30:      Mark the evicting block has been deleted
 * bit 31:      Evicting bucket will set this bit to 1
 */
typedef struct ShareMemBucket {
    uint32_t flags;
    FileId fileId;
    int16_t usageCount;
    int32_t fileBlockIndex;
    int16_t evictLoadActiveId;
    BucketActiveInfo activeInfos[SMBUCKET_MAX_CONCURRENT_OPEN];

    /* Bucket status operations */
    bool isFreeBucket() { return (flags & 0x00000003) == 0 ? true : false; };
    bool isActiveBucket() { return (flags & 0x00000003) == 1 ? true : false; };
    bool isUsedBucket() { return (flags & 0x00000003) == 2 ? true : false; };
    bool isEvictingBucket() { return (flags & 0x80000000); };
    bool isDeletedBucket() { return (flags & 0x40000000); };
    bool isLoadingBucket() { return (flags & 0x20000000); };
    void setBucketFree() { flags = (flags & BucketTypeMask) | 0x00000000; };
    void setBucketActive() { flags = (flags & BucketTypeMask) | 0x00000001; };
    void setBucketUsed() { flags = (flags & BucketTypeMask) | 0x00000002; };
    void setBucketEvicting() { flags = (flags | 0x80000000); };
    void setBucketEvictFinish() { flags = (flags & 0x7FFFFFFF); };
    void setBucketDeleted() { flags = (flags | 0x40000000); };
    void setBucketLoading() { flags = (flags | 0x20000000); };
    void setBucketLoadFinish() { flags = (flags & 0xDFFFFFFF); };

    void reset();
    void markWrite(int activeId);
    void markRead(int activeId);
    void unmarkWrite(int16_t activeId);
    void unmarkRead(int16_t activeId);
    bool noActiveReadWrite();
} ShareMemBucket;

/* This field is to support multiple-read and protect single-wirte
 * Each ActiveStatus will regist here when constructing, and set the
 * evicting/loading status to let others know the overall status.
 * Operations are:
 * 1. Check whether all ActiveStatus of a File is closed
 * 2. Check the evicting ActiveStatus pid
 * 3. Check the loading status(from OSS) of a file block
 *
 * FLAGS (low -> high):
 * 0  bit: mark evicting
 * 1  bit: mark loading
 * 29 bit: mark the activeStatus opened file has been unlinked, should destroy when
 *          closing this activestatus if it's the last opened activestatus
 * 30 bit: mark the evict bucket has been stolen(the owner get it back)
 * 31 bit: mark the evict bucket has been deleted(the owner file has been deleted)
 */
typedef struct ShareMemActiveStatus {
    int pid;
    int32_t flags;
    FileId fileId;
    FileId evictFileId;
    int32_t fileBlockIndex;

    void setEvicting() { flags |= 0x00000001; };
    void setLoading() { flags |= 0x00000002; };
    void setForDelete() { flags |= 0x80000000; };
    void setBucketStolen() { flags |= 0x40000000; };
    void setShouldDestroy() { flags |= 0x20000000; };
    void unsetEvicting() { flags &= 0xFFFFFFFE; };
    void unsetLoading() { flags &= 0xFFFFFFFD; };
    void unsetBucketStolen() { flags &= 0xBFFFFFFF; };
    void unsetShouldDestroy() { flags &= 0xDFFFFFFF; };

    bool isForDelete() { return flags & 0x80000000; };
    bool isEvictBucketStolen() { return flags & 0x40000000; };
    bool shouldDestroy() { return flags & 0x20000000; };

    void reset() {
        pid = InvalidPid;
        flags = 0;
        fileId.reset();
        evictFileId.reset();
        fileBlockIndex = InvalidBlockId;
    };
} ShareMemActiveStatus;

}
}
#endif //GOPHERWOOD_CORE_SHAREDMEMORYOBJ_H
