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
#include "common/Configuration.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Memory.h"
#include "common/Logger.h"
#include "core/Manifest.h"

#include <sys/fcntl.h>

namespace Gopherwood {
namespace Internal {

int64_t Manifest::BUFFER_SIZE = 4 * 1024;

Manifest::Manifest(std::string path) :
        mFilePath(path), mFD(-1) {
    mfOpen();
    mfSeek(0, SEEK_SET);
    mBuffer = (char *) malloc(BUFFER_SIZE);
}

std::string Manifest::getManifestFileName(std::string workDir, FileId fileId) {
    std::stringstream ss;
    ss << workDir << Configuration::MANIFEST_FOLDER << '/' << fileId.hashcode << '-'
       << fileId.collisionId;
    return ss.str();
}

void Manifest::logAcquireNewBlock(std::vector<Block> &blocks) {
    /* build Acquire New Block Opaque */
    RecOpaque opaque;
    opaque.acquireNewBlock.padding = 0;

    /* build log record */
    std::string logRecord = serializeManifestLog(blocks, RecordType::acquireNewBlock, opaque);

    /* flush to log */
    mfWrite(logRecord);
    LOG(DEBUG1, "[Manifest]              |"
              "New acquireNewBlock log record");
}

void Manifest::logExtendBlock(std::vector<Block> &blocks, RecOpaque opaque) {
    /* build log record */
    std::string logRecord = serializeManifestLog(blocks, RecordType::extendBlock, opaque);

    /* flush to log */
    mfWrite(logRecord);
    LOG(DEBUG1, "[Manifest]              |"
              "New extendBlock log record");
}

void Manifest::logUpdateEof(RecOpaque opaque) {
    /* Empty vector */
    std::vector<Block> blocks;

    /* build log record */
    std::string logRecord = serializeManifestLog(blocks, RecordType::updateEof, opaque);

    /* flush to log */
    mfWrite(logRecord);
    LOG(DEBUG1, "[Manifest]              |"
              "New updateEof log record");
}

void Manifest::logReleaseBucket(std::list<Block> &blocks) {
    /* build Acquire New Block Opaque */
    RecOpaque opaque;
    opaque.common.padding = 0;

    std::vector<Block> logBlocks;
    for (Block b : blocks){
        logBlocks.push_back(b);
    }

    /* build log record */
    std::string logRecord = serializeManifestLog(logBlocks, RecordType::releaseBlock, opaque);

    /* flush to log */
    mfWrite(logRecord);
    LOG(DEBUG1, "[Manifest]              |"
              "New releaseBlock log record");
}

void Manifest::logInactivateBucket(std::vector<Block> &blocks) {
    /* build Acquire New Block Opaque */
    RecOpaque opaque;
    opaque.common.padding = 0;

    /* build log record */
    std::string logRecord = serializeManifestLog(blocks, RecordType::inactiveBlock, opaque);

    /* flush to log */
    mfWrite(logRecord);
    LOG(DEBUG1, "[Manifest]              |"
              "New inactiveBlock log record");
}

void Manifest::logActivateBucket(Block &block) {
    /* build Acquire New Block Opaque */
    RecOpaque opaque;
    opaque.common.padding = 0;

    std::vector<Block> blocks;
    blocks.push_back(block);

    /* build log record */
    std::string logRecord = serializeManifestLog(blocks, RecordType::activeBlock, opaque);

    /* flush to log */
    mfWrite(logRecord);
    LOG(DEBUG1, "[Manifest]              |"
            "New activeBlock log record");
}

void Manifest::logEvcitBlock(Block &block) {
    /* build Acquire New Block Opaque */
    RecOpaque opaque;
    opaque.common.padding = 0;

    std::vector<Block> blocks;
    blocks.push_back(block);

    /* build log record */
    std::string logRecord = serializeManifestLog(blocks, RecordType::evictBlock, opaque);

    /* flush to log */
    mfWrite(logRecord);
    LOG(DEBUG1, "[Manifest]              |"
            "New evictBlock log record");
}

void Manifest::logLoadBlock(Block &block) {
    /* build load Block Opaque */
    RecOpaque opaque;
    opaque.common.padding = 0;

    std::vector<Block> blocks;
    blocks.push_back(block);

    /* build log record */
    std::string logRecord = serializeManifestLog(blocks, RecordType::loadBlock, opaque);

    /* flush to log */
    mfWrite(logRecord);
    LOG(DEBUG1, "[Manifest]              |"
            "New loadBlock log record");
}

void Manifest::logFullStatus(std::vector<Block> &blocks, RecOpaque opaque) {
    /* truncate existing Manifest file */
    mfTruncate();

    /* build log record */
    std::string logRecord = serializeManifestLog(blocks, RecordType::fullStatus, opaque);

    /* flush to log */
    mfWrite(logRecord);
    LOG(DEBUG1, "[Manifest]              |"
              "New fullStatus log record");
}

RecordHeader Manifest::fetchOneLogRecord(std::vector<Block> &blocks) {
    RecordHeader header;
    int64_t bytesRead;

    /* get log size and eyecatcher */
    bytesRead = mfRead(mBuffer, 10);
    if (bytesRead == 0) {
        header.type = RecordType::invalidLog;
        return header;
    }

    int64_t recLength = *(int64_t *) mBuffer;
    uint16_t eyecatcher = *(uint16_t *) (mBuffer + 8);
    if (recLength > BUFFER_SIZE) {
        THROW(GopherwoodException,
              "[Manifest::fetchOneLogRecord] recLength should not exceed buffer size.");
    }
    if (bytesRead == -1 ||
        bytesRead != 10 ||
        eyecatcher != MANIFEST_RECORD_EYECATCHER) {
        THROW(GopherwoodException, "[Manifest::fetchOneLogRecord] read log error, fd=%dbytesRead=%ld",
              mFD, bytesRead);
    }

    /* read the whole log record to buffer */
    bytesRead = mfRead(mBuffer + 10, recLength - 10);
    if (bytesRead != recLength - 10) {
        THROW(GopherwoodException, "[Manifest::fetchOneLogRecord] read log error.");
    }

    /* build log header and block info */
    header = *(RecordHeader *) mBuffer;
    uint32_t numBlocks = 0;
    BlockRecord *blockRecord = (BlockRecord *) (mBuffer + sizeof(RecordHeader));
    while (numBlocks < header.numBlocks) {
        Block block = blockRecord->toBlockFormat();
        blocks.push_back(block);
        blockRecord++;
        numBlocks++;
    }

    return header;
}

void Manifest::flush() {

}

void Manifest::destroy() {
    mfClose();
    mfRemove();
}

std::string Manifest::serializeManifestLog(std::vector<Block> &blocks, RecordType type, RecOpaque opaque) {
    /* build log record header */
    std::string logRecord;

    RecordHeader header;
    header.recordLength = sizeof(RecordHeader) + blocks.size() * sizeof(BlockRecord);
    header.eyecatcher = MANIFEST_RECORD_EYECATCHER;
    header.type = type;
    header.flags = 0;
    header.opaque = opaque;
    header.numBlocks = blocks.size();

    /* build header string */
    std::string headerStr;
    char buf[sizeof(RecordHeader)];
    memcpy(buf, &header, sizeof(RecordHeader));
    headerStr.append(buf, sizeof(buf));

    /* build the log record */
    std::stringstream ss;
    ss << headerStr;
    for (unsigned int i = 0; i < blocks.size(); i++) {
        ss << blocks[i].toLogFormat();
    }

    logRecord = ss.str();
    if (logRecord.size() != header.recordLength) {
        THROW(GopherwoodIOException,
              "[Manifest::serializeManifestLog] Broken log record, expect_size=%lu, actual_size=%lu",
              header.recordLength, logRecord.size());
    }

    return logRecord;
}

/************************************************************
 *      Support Functions For Manifest File Operations      *
 ************************************************************/
void Manifest::mfOpen() {
    int flags = O_CREAT | O_RDWR;
    mFD = open(mFilePath.c_str(), flags, 0644);
    if (mFD == -1){
        THROW(GopherwoodIOException,
              "[Manifest::mfOpen] open failed %s.",
              mFilePath.c_str());
    }
}

void Manifest::mfSeek(int64_t offset, int flag) {
    lseek(mFD, offset, flag);
}

void Manifest::mfWrite(std::string &record) {
    int len = write(mFD, record.c_str(), record.size());
    if (len == -1 || len != (int) record.size()) {
        THROW(GopherwoodIOException,
              "[Manifest::mfWrite] write failed %s.",
              mFilePath.c_str());
    }
}

inline int64_t Manifest::mfRead(char *buffer, int64_t size) {
    return read(mFD, buffer, size);
}

void Manifest::mfTruncate() {
    ftruncate(mFD, 0);
    mfSeek(0, SEEK_SET);
}

void Manifest::lock() {
    lockf(mFD, F_LOCK, 0);
}

void Manifest::unlock() {
    lockf(mFD, F_ULOCK, 0);
}

void Manifest::mfClose() {
    if (mFD != -1) {
        close(mFD);
    }
    mFD = -1;
}

void Manifest::mfRemove() {
    remove(mFilePath.c_str());
}

Manifest::~Manifest() {
    if (mBuffer) {
        free(mBuffer);
        mBuffer = NULL;
    }
    mfClose();
}

}
}