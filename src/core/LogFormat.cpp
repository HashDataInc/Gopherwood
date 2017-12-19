//
// Created by root on 12/12/17.
//

#include <iostream>
#include "LogFormat.h"
#include "Logger.h"
#include <unistd.h>
#include <string.h>


#include "../common/Logger.h"
#include "../util/Coding.h"


namespace Gopherwood {
    namespace Internal {

        LogFormat::LogFormat() {

        }

        LogFormat::~LogFormat() {

        }


        std::string LogFormat::serializeBlockIDVector(const std::vector<int32_t> &blockIdVector) {
            std::string res;
            PutFixed32(&res, blockIdVector.size());

            LOG(INFO, "1, LogFormat res size = %d", res.size());
            for (int i = 0; i < blockIdVector.size(); i++) {
                PutFixed32(&res, blockIdVector[i]);
                LOG(INFO, "2, LogFormat res size = %d", res.size());
            }
            return res;
        }


        std::string
        LogFormat::serializeHeaderAndBlockIds(const std::vector<int32_t> &blockIdVector, RecordType recordType) {
            std::string tmpStr = serializeBlockIDVector(blockIdVector);
            std::string res;

            if (recordType == RecordType::acquireNewBlock) {
                PutFixed32(&res, tmpStr.size() + 1 + 4); //total size = 1(type)+tmpStr.size()+4(pid)
            } else {
                PutFixed32(&res, tmpStr.size() + 1); //total size = 1(type)+tmpStr.size()
            }

            res.append(1, static_cast<char>(recordType));

            LOG(INFO, "3, LogFormat res size = %d", res.size());
            res.append(tmpStr.data(), tmpStr.size());
            return res;
        }


        std::string
        LogFormat::serializeAcquireNewBlock(const std::vector<int32_t> &blockIdVector, RecordType recordType) {
            std::string res = serializeHeaderAndBlockIds(blockIdVector, recordType);
            int pid = getpid();
            PutFixed32(&res, pid);

            LOG(INFO, "4, LogFormat res size = %d, pid = %d ", res.size(), pid);
            return res;
        }


        std::string
        LogFormat::serializeInactiveBlock(const std::vector<int32_t> &blockIdVector, RecordType recordType) {
            std::string res = serializeHeaderAndBlockIds(blockIdVector, recordType);
            return res;
        }

        std::string LogFormat::serializeReleaseBlock(const std::vector<int32_t> &blockIdVector, RecordType recordType) {
            std::string res = serializeHeaderAndBlockIds(blockIdVector, recordType);
            return res;
        }

        std::string LogFormat::serializeEvictBlock(const std::vector<int32_t> &blockIdVector, RecordType recordType) {
            std::string res = serializeHeaderAndBlockIds(blockIdVector, recordType);
            return res;
        }

        std::string LogFormat::serializeRemoteBlock(const std::vector<int32_t> &blockIdVector, RecordType recordType) {
            std::string res = serializeHeaderAndBlockIds(blockIdVector, recordType);
            return res;
        }


        std::string LogFormat::serializeLog(const std::vector<int32_t> &blockIdVector, RecordType type) {
            LOG(INFO, "LogFormat::serializeLog, and the RecordType =  %d, blockIdVector size = %d", type,
                blockIdVector.size());
            switch (type) {
                case acquireNewBlock:
                    return serializeAcquireNewBlock(blockIdVector, type);
                case inactiveBlock:
                    return serializeInactiveBlock(blockIdVector, type);
                case releaseBlock:
                    return serializeReleaseBlock(blockIdVector, type);
                case evictBlock:
                    return serializeEvictBlock(blockIdVector, type);
                case remoteBlock:
                    return serializeRemoteBlock(blockIdVector, type);
            }
        }


        std::string LogFormat::serializeFileStatusForClose(shared_ptr<FileStatus> fileStatus){
            std::string res;
            int32_t size = 1+4+fileStatus->getBlockIdVector().size()*4+8;
            PutFixed32(&res, size);
            res.append(1, static_cast<char>(RecordType::closeFile));
            PutFixed32(&res, fileStatus->getBlockIdVector().size());
            for(int i=0;i<fileStatus->getBlockIdVector().size();i++){
                PutFixed32(&res, fileStatus->getBlockIdVector()[i]);
            }
            PutFixed32(&res, fileStatus->getLastBucket());
        }

        void LogFormat::deserializeLog(std::string val, shared_ptr<FileStatus> fileStatus) {

            char *res = (char *) val.data();

            char type = *res;

            int iType = type;
            LOG(INFO, "deserializeLog  int iType   = %d", iType);
            switch (iType) {
                case acquireNewBlock:
                    deserializeAcquireNewBlock(val, fileStatus);
                    break;
                case inactiveBlock:
                    deserializeInactiveBlock(val, fileStatus);
                    break;
                case releaseBlock:
                    deserializeReleaseBlock(val, fileStatus);
                    break;
                case evictBlock:
                    deserializeEvictBlock(val, fileStatus);
                    break;
                case remoteBlock:
                    deserializeRemoteBlock(val, fileStatus);
                    break;
                case closeFile:
                    deserializeCloseFile(val, fileStatus);
                    break;
            }
        }


        int LogFormat::deserializeHeaderAndBlockIds(std::string val, shared_ptr<FileStatus> fileStatus) {
            char *res = (char *) val.data();
            char type = *res;
            int iType = type;
            LOG(INFO, "deserializeHeaderAndBlockIds  type   = %d", iType);

            std::vector<int32_t> tmpVector;

            int offset = 1;
            int32_t numOfBlocks = DecodeFixed32(res + offset);
            offset += 4;
            LOG(INFO, "deserializeHeaderAndBlockIds numOfBlocks  = %d", numOfBlocks);

            for (int i = 0; i < numOfBlocks; i++) {
                int32_t blockID = DecodeFixed32(res + offset);
                tmpVector.push_back(blockID);
                LOG(INFO, "deserializeHeaderAndBlockIds numOfBlocks id  = %d", blockID);
                offset += 4;
            }

            if (iType == 0) { // acquire new block
                fileStatus->setBlockIdVector(tmpVector);
            } else if (iType == 2) { // releaseBlock
                for (int i = 0; i < tmpVector.size(); i++) {
                    int blockID2Release = tmpVector[i];
                    std::vector<int32_t>::iterator it;
                    for (it = fileStatus->getBlockIdVector().begin(); it != fileStatus->getBlockIdVector().end();) {
                        if (*it == blockID2Release) {
                            it = fileStatus->getBlockIdVector().erase(it);
                            break;
                        } else {
                            it++;
                        }
                    }
                }

            } else if (iType == 3) { // evict block, this means the block now belong to the new file,
                for (int i = 0; i < tmpVector.size(); i++) {
                    int blockID2Add = tmpVector[i];
                    fileStatus->getBlockIdVector().push_back(blockID2Add);
                }
            } else if (iType == 4) { //remoteBlock
                for (int i = 0; i < tmpVector.size(); i++) {
                    int blockIDRemote = tmpVector[i];
                    if (-blockIDRemote > fileStatus->getBlockIdVector().size()) {
                        LOG(LOG_ERROR, "error occur, the remote block index can not larger than the block vector");
                    }
                    fileStatus->getBlockIdVector()[-blockIDRemote] = -blockIDRemote;
                }
            }
            return offset;
        }


        void LogFormat::deserializeAcquireNewBlock(std::string val, shared_ptr<FileStatus> fileStatus) {
            LOG(INFO, "*******deserializeAcquireNewBlock*******");
            int offset = deserializeHeaderAndBlockIds(val, fileStatus);
            char *res = (char *) val.data();
            int pid = DecodeFixed32(res + offset);
            LOG(INFO, "deserializeAcquireNewBlock pid   = %d", pid);
            LOG(INFO, "*******deserializeAcquireNewBlock*******");
        }


        void LogFormat::deserializeInactiveBlock(std::string val, shared_ptr<FileStatus> fileStatus) {
            LOG(INFO, "*******deserializeInactiveBlock*******");
            int offset = deserializeHeaderAndBlockIds(val, fileStatus);
            LOG(INFO, "*******deserializeInactiveBlock*******");
        }

        void LogFormat::deserializeReleaseBlock(std::string val, shared_ptr<FileStatus> fileStatus) {
            LOG(INFO, "*******deserializeReleaseBlock*******");
            int offset = deserializeHeaderAndBlockIds(val, fileStatus);
            LOG(INFO, "*******deserializeReleaseBlock*******");
        }

        void LogFormat::deserializeEvictBlock(std::string val, shared_ptr<FileStatus> fileStatus) {
            LOG(INFO, "*******deserializeEvictBlock*******");
            int offset = deserializeHeaderAndBlockIds(val, fileStatus);
            LOG(INFO, "*******deserializeEvictBlock*******");
        }

        void LogFormat::deserializeRemoteBlock(std::string val, shared_ptr<FileStatus> fileStatus) {
            LOG(INFO, "*******deserializeRemoteBlock*******");
            int offset = deserializeHeaderAndBlockIds(val, fileStatus);
            LOG(INFO, "*******deserializeRemoteBlock*******");
        }

        //TODO , this is not complete
        void LogFormat::deserializeCloseFile(std::string val, shared_ptr<FileStatus> fileStatus) {
            LOG(INFO, "*******deserializeCloseFile*******");
            int offset = deserializeHeaderAndBlockIds(val, fileStatus);
            LOG(INFO, "*******deserializeCloseFile*******");
        }
    }
}


//header
/**
-----------------------------------------------------
|size of total data| type                           |
-----------------------------------------------------
|   4536           |RecordType::acquireNewBlock     |
-----------------------------------------------------
|   4 byte         | 1 byte                         |
**/




//  type=acquireNewBlock           0->1
/**
----------------------------------------------------
| num. of blocks| block 1 | block 2 | .....| pid   |
----------------------------------------------------
|   3           |     1   |   5     | .....| 68430 |
---------------------------------------------------|
|   4 byte      |  4 byte | 4 byte  |4 byte| 4 byte|
**/


//  type=inactiveBlock            1->2
/**
--------------------------------------------
| num. of blocks| block 1 | block 2 | .....|
--------------------------------------------
|   3           |     1   |   5     | .....|
--------------------------------------------
|   4 byte      |  4 byte | 4 byte  |4 byte|
**/

//  type=releaseBlock             1->0
/**
--------------------------------------------
| num. of blocks| block 1 | block 2 | .....|
--------------------------------------------
|   3           |     1   |   5     | .....|
--------------------------------------------
|   4 byte      |  4 byte | 4 byte  |4 byte|
**/


//  type=evictBlock             2->1
/**
--------------------------------------------
| num. of blocks| block 1 | block 2 | .....|
--------------------------------------------
|   3           |     1   |   5     | .....|
--------------------------------------------
|   4 byte      |  4 byte | 4 byte  |4 byte|
**/



//  type=remoteBlock            block id = - block id
/**
--------------------------------------------
| num. of blocks| block 1 | block 2 | .....|
--------------------------------------------
|   3           |    -1   |  -5     | .....|
--------------------------------------------
|   4 byte      |  4 byte | 4 byte  |4 byte|
**/




//  type=closeFile            FileStatus serializeFileStatus

/**
---------------------------------------------------------------
| num. of blocks| block 1 | block 2 | .... |endOffsetOfBucket |
---------------------------------------------------------------
|   3           |     1   |   5     |    6 |45062             |
---------------------------------------------------------------
|   4 byte      |  4 byte | 4 byte  |4 byte|8 byte            |
**/

