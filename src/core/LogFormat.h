#ifndef _GOPHERWOOD_CORE_LOGFORMAT_H_
#define _GOPHERWOOD_CORE_LOGFORMAT_H_

#include <vector>
#include <stdio.h>
#include <stdint.h>
#include <string>
#include <memory>
#include "FileStatus.h"

namespace Gopherwood {
namespace Internal {

class LogFormat {

public:
    LogFormat();

    ~LogFormat();

    enum RecordType {

        acquireNewBlock = 0, // block type change 0 to 1

        inactiveBlock = 1, // block type change 1 to 2

        releaseBlock = 2, // block type change 1 to 0

        evictBlock = 3, // block move to OSS, so the type change 2 to a negative id in OSS,
        // and this block can be used by other apps, and the block will be changed 1 in other apps log

        remoteBlock = 4,                // the block lists is in the OSS, the id is negative

        closeFile = 5,     // the file have been closed and the FileStatus are collected and merged.

        deleteBlock = 6 // delete one block. used in the block which we wants to cache is in the OSS

    };
    // Header is length (4 bytes), type (1 byte)
    const int kHeaderSize = 4 + 1;

private:
//            int length; // the length of the type;
//            RecordType type;
//            char data[1]; // the real data

public:
    std::string serializeLog(const std::vector<int32_t> &blockIdVector, RecordType type);

    void deserializeLog(std::string val, shared_ptr<FileStatus> fileStatus);

    std::string serializeFileStatusForClose(shared_ptr<FileStatus> fileStatus);

private:
    std::string serializeBlockIDVector(const std::vector<int32_t> &blockIdVector);

    std::string serializeAcquireNewBlock(const std::vector<int32_t> &blockIdVector,
            RecordType type);

    std::string serializeHeaderAndBlockIds(const std::vector<int32_t> &blockIdVector,
            RecordType type);

    std::string serializeInactiveBlock(const std::vector<int32_t> &blockIdVector, RecordType type);

    std::string serializeReleaseBlock(const std::vector<int32_t> &blockIdVector, RecordType type);

    std::string serializeEvictBlock(const std::vector<int32_t> &blockIdVector, RecordType type);

    std::string serializeRemoteBlock(const std::vector<int32_t> &blockIdVector, RecordType type);

    std::string serializeDeleteBlock(const std::vector<int32_t> &blockIdVector,
            RecordType recordType);

    std::string serializeCloseFile(const std::vector<int32_t> &blockIdVector,
            RecordType recordType);

    int deserializeHeaderAndBlockIds(std::string val, shared_ptr<FileStatus> fileStatus);

    void deserializeAcquireNewBlock(std::string val, shared_ptr<FileStatus> fileStatus);

    void deserializeInactiveBlock(std::string val, shared_ptr<FileStatus> fileStatus);

    void deserializeDeleteBlock(std::string val, shared_ptr<FileStatus> fileStatus);

    void deserializeReleaseBlock(std::string val, shared_ptr<FileStatus> fileStatus);

    void deserializeEvictBlock(std::string val, shared_ptr<FileStatus> fileStatus);

    void deserializeRemoteBlock(std::string val, shared_ptr<FileStatus> fileStatus);

    void deserializeCloseFile(std::string val, shared_ptr<FileStatus> fileStatus);
};

}
}

#endif //_GOPHERWOOD_CORE_LOGFORMAT_H_
