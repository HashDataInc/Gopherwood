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
#ifndef GOPHERWOOD_CORE_MANIFEST_H
#define GOPHERWOOD_CORE_MANIFEST_H

#include "file/FileId.h"
#include "core/BlockStatus.h"

namespace Gopherwood {
namespace Internal {

enum RecordType {
    /* acquire new blocks to activestatus pin list
     * transit Shared Memory state from 0 to 1 */
    acquireNewBlock = 0,
    /* inactive a file block from activestatus
     * transit Shared Memory state from 1 to 2 */
    inactiveBlock = 1,
    /* active a file block, add to activestatus
     * transit Shared Memory state from 2 to 1 */
    activeBlock = 2,
    /* release a file block from activestatus
     * transit Shared Memory state from 1 to 0 */
    releaseBlock = 3,
    /* change a file block status
     * transit Shared Memory state from 1 to 0 */
    blockUpdate = 4,
    /* assign a block to local bucket */
    assignBlock = 5,
    /* evict a block from local space to OSS
     * transit Shared Memory state from 2 to 0 */
    evictBlock = 6,
    /* the file have been closed and the FileStatus
     * are collected and merged. */
    fullStatus = 7
};

struct Common {
    int32_t padding;
};

struct AcquireNewBlock {
    int32_t padding;
};

struct ExtendBlock {
    int32_t padding;
};

typedef union RecOpaque {
    Common  common;
    AcquireNewBlock acquireNewBlock;
    ExtendBlock  extendBlock;
} RecOpaque;

/* this is a random prime number to check log record integrity */
#define MANIFEST_RECORD_EYECATCHER 0xCAED

struct RecordHeader {
    uint64_t recordLength;
    uint16_t eyecatcher;
    uint8_t type;
    uint8_t flags;
    uint32_t numBlocks;
    RecOpaque opaque;

    std::string toLogFormat();
};

class Manifest {
public:
    Manifest(std::string path);

    void logAcquireNewBlock(std::vector<Block> &blocks);

    void logExtendBlock(std::vector<Block> &blocks);

    void logFullStatus(std::vector<Block> &blocks);

    void catchUpLog();

    void flush();

    void lock();

    void unlock();

    ~Manifest();

private:
    std::string serializeManifestLog(std::vector<Block> &blocks, RecordType type, RecOpaque opaque);

    inline void mfOpen();

    inline void mfSeekEnd();

    inline void mfAppend(std::string &record);

    inline void mfTruncate();

    inline void mfClose();

    std::string mFilePath;
    int mFD;

};

}
}
#endif //GOPHERWOOD_CORE_MANIFEST_H