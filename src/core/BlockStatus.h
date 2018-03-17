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
#ifndef GOPHERWOOD_BLOCKSTATUS_H
#define GOPHERWOOD_BLOCKSTATUS_H

#include "platform.h"
#include "common/Memory.h"
#include "file/FileId.h"

namespace Gopherwood {
namespace Internal {

#define InvalidBlockId  -1
#define LocalBlock      true
#define RemoteBlock     false

/* The in-memory file block info */
typedef struct Block {
    /* The bucket id in local cache space */
    int32_t bucketId;
    /* The block id of Gopherwood File */
    int32_t blockId;
    /* Ture if current block is in OSS */
    bool isLocal;
    /* Current bucket status of this block, only
     * meaningful when block is in local cache space */
    uint8_t state;
    /* Ture if the File already got the authority to manipulate this block  */
    bool isMyActive;

    Block(int32_t theBucketId, int32_t theBlockId, bool local, uint8_t s, bool myActive);
    std::string toLogFormat();
} Block;

#define BLOCK_RECORD_REMOTE     0x8000

#define BLOCK_RECORD_TYPE_MASK  0x0003
#define BLOCK_RECORD_FREE       0x0000
#define BLOCK_RECORD_ACTIVE     0x0001
#define BLOCK_RECORD_USED       0x0002

/* The Block info for Manifest Log format */
typedef struct BlockRecord {
    /* Compact format of block status
     * 0~1 bits: Bucket type if the block is in local cache space
     * 15  bit : Mark the block is in OSS or in local cache space*/
    uint16_t rFlags;
    uint16_t rPadding;
    /* The bucket id in local cache space */
    int32_t rBucketId;
    /* The block id of Gopherwood File */
    int32_t rBlockId;

    Block toBlockFormat();
} BlockRecord;

#define InvalidBlockOffset -1

typedef struct BlockInfo {
    FileId fileId;
    int32_t blockId;
    int64_t offset;
    bool isLocal;
} BlockInfo;

}
}
#endif //GOPHERWOOD_BLOCKSTATUS_H
