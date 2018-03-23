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
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "core/BlockStatus.h"
#include "SharedMemoryContext.h"

namespace Gopherwood {
namespace Internal {

Block::Block(int32_t theBucketId, int32_t theBlockId, bool local, uint8_t s) :
        bucketId(theBucketId), blockId(theBlockId), isLocal(local), state(s) {

}

std::string Block::toLogFormat() {
    std::string res("");
    BlockRecord record;

    /* build flags */
    record.rFlags = 0;
    if (isLocal == RemoteBlock) {
        record.rFlags |= BLOCK_RECORD_REMOTE;
    }
    switch (state) {
        /* No need to set bucket status free, since it's 0 */
        case BUCKET_FREE:
            record.rFlags |= BLOCK_RECORD_FREE;
            break;
        case BUCKET_ACTIVE:
            record.rFlags |= BLOCK_RECORD_ACTIVE;
            break;
        case BUCKET_USED:
            record.rFlags |= BLOCK_RECORD_USED;
            break;
        default:
            THROW(GopherwoodException,
                  "[Block::toLogFormat] Unrecognized BlockState %d",
                  state);
    }

    record.rBucketId = bucketId;
    record.rBlockId = blockId;

    char buf[sizeof(BlockRecord)];
    memcpy(buf, &record, sizeof(BlockRecord));
    res.append(buf, sizeof(buf));

    return res;
}

Block BlockRecord::toBlockFormat() {
    uint8_t state = BUCKET_FREE;

    bool isLocal = rFlags & BLOCK_RECORD_REMOTE ? RemoteBlock : LocalBlock;
    int type = rFlags & BLOCK_RECORD_TYPE_MASK;

    switch (type) {
        case BLOCK_RECORD_FREE:
            state = BUCKET_FREE;
            break;
        case BLOCK_RECORD_ACTIVE:
            state = BUCKET_ACTIVE;
            break;
        case BLOCK_RECORD_USED:
            state = BUCKET_USED;
            break;
        default:
            THROW(GopherwoodException,
                  "[Block::toBlockFormat] Unrecognized BlockRecordState %d",
                  state);
    }

    return Block(rBucketId, rBlockId, isLocal, state);
}

}
}