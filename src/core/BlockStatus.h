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

namespace Gopherwood {
namespace Internal {


typedef struct Block {
    int32_t     bucketId;
    int32_t     blockId;
    bool        isLocal;
    uint8_t     state;

    Block(int32_t theBucketId, int32_t theBlockId, bool local, uint8_t s) :
            bucketId(theBucketId),
            blockId(theBlockId),
            isLocal(local),
            state(s) {};
} Block;

typedef struct BlockInfo {
    int32_t id;
    int64_t offset;
    bool    isLocal;
} BlockInfo;

}
}
#endif //GOPHERWOOD_BLOCKSTATUS_H
