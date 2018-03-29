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
    /* acquire new blocks to active status pin list
     * transit Shared Memory state from 0 to 1 */
    acquireNewBlock = 0,
    /* inactive a file block from active status
     * transit Shared Memory state from 1 to 2 */
    inactiveBlock = 1,
    /* active a file block, add to active status
     * transit Shared Memory state from 2 to 1 */
    activeBlock = 2,
    /* release a file block from active status
     * transit Shared Memory state from 1 to 0 */
    releaseBlock = 3,
    /* load a block from OSS to local space */
    loadBlock = 4,
    /* assign a block to local bucket */
    extendBlock = 5,
    /* evict a block from local space to OSS
     * transit Shared Memory state from 2 to 0 */
    evictBlock = 6,
    /* the file have been closed and the FileStatus
     * are collected and merged. */
    fullStatus = 7,
    /* Update the file EOF when flush API is called */
    updateEof = 8,
    invalidLog = 100
};

struct Common {
    int64_t padding;
};

struct AcquireNewBlock {
    int64_t padding;
};

struct ExtendBlock {
    int64_t eof;
};

struct FullStatus {
    int64_t eof;
};

struct UpdateEof {
    int64_t eof;
};

/* The Manifest Log data field for different log types */
typedef union RecOpaque {
    Common common;
    AcquireNewBlock acquireNewBlock;
    ExtendBlock extendBlock;
    FullStatus fullStatus;
    UpdateEof updateEof;
} RecOpaque;

/* this is a random prime number to check log record integrity */
#define MANIFEST_RECORD_EYECATCHER 0xCAED

/* A Manifest Log contains a RecordHeader and a number of BlockRecords */
struct RecordHeader {
    /* The total length of header and blocks */
    uint64_t recordLength;
    /* The safe guard of each log record */
    uint16_t eyecatcher;
    /* Log Record Type */
    uint8_t type;
    uint8_t flags;
    /* Number of blocks in this log record */
    uint32_t numBlocks;
    /* The data for each type of log records */
    RecOpaque opaque;

    std::string toLogFormat();
};

class Manifest {
public:
    Manifest(std::string path);

    static std::string getManifestFileName(std::string workDir, FileId fileId);

    void logAcquireNewBlock(std::vector<Block> &blocks);
    void logExtendBlock(std::vector<Block> &blocks, RecOpaque opaque);
    void logFullStatus(std::vector<Block> &blocks, RecOpaque opaque);
    void logUpdateEof(RecOpaque opaque);
    void logReleaseBucket(std::list<Block> &blocks);
    void logInactivateBucket(std::vector<Block> &blocks);
    void logActivateBucket(Block &block);
    void logEvcitBlock(Block &block);
    void logLoadBlock(Block &block);

    RecordHeader fetchOneLogRecord(std::vector<Block> &blocks);

    void mfSeek(int64_t offset, int flag);
    void flush();
    void lock();
    void unlock();
    void destroy();

    ~Manifest();

private:
    static int64_t BUFFER_SIZE;

    std::string serializeManifestLog(std::vector<Block> &blocks, RecordType type, RecOpaque opaque);

    /******************** File Operations ********************/
    inline void mfOpen();
    inline void mfWrite(std::string &record);
    inline int64_t mfRead(char *buffer, int64_t size);
    inline void mfTruncate();
    inline void mfClose();
    inline void mfRemove();

    /******************** Fields ********************/
    std::string mFilePath;
    int mFD;
    int64_t mPos;
    int64_t mReadPos;
    char *mBuffer;
};

}
}
#endif //GOPHERWOOD_CORE_MANIFEST_H
