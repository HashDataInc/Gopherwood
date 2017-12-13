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


        /**
        std::string LogFormat::serializeBlockIDVector(const std::vector<int32_t> &blockIdVector) {
            std::string res;


//            int totalLength = 4+ 4 * (int) blockIdVector.size();

//            char *res = (char *) (malloc(totalLength));

            char res[totalLength];


            int length = 0;
            PutVarint32(&res, blockIdVector.size());

            int *tmp = (int *) (res + length);

            *tmp = (int) blockIdVector.size();
            length += 4;

            for (int i = 0; i < blockIdVector.size(); i++) {
                PutVarint32(&res, blockIdVector[i]);
                tmp = (int *) (res + length);
                *tmp = blockIdVector[i];
                LOG(INFO, "*********blockIdVector= %d", *((int *) (res + length)));
                length += 4;
            }



            //****************test for output**********************


            int tmpll = 0;

            int tmpTotalSizeOfBlockID = *((int *) (res + tmpll));
            tmpll += 4;
            LOG(INFO, "tmpTotalSizeOfBlockID  size = %d", tmpTotalSizeOfBlockID);
            for (int i = 0; i < tmpTotalSizeOfBlockID; i++) {
                int tmpBlockID = *((int *) (res + tmpll));
                LOG(INFO, "&&&&&&&&&&&&tmpBlockID= %d", tmpBlockID);
                tmpll += 4;
            }
            //****************test for output******************

            LOG(INFO, "serializeBlockIDVector   sizeof = %d", sizeof(res));
            LOG(INFO, "serializeBlockIDVector   strlen = %d", strlen(res));
            return res;
        }

**/




        /**
        std::string LogFormat::serializeAcquireNewBlock(const std::vector<int32_t> &blockIdVector) {
            std::string tmpStr = serializeBlockIDVector(blockIdVector);
            char *tmpRes = (char*)tmpStr.data();
            LOG(INFO, "blockIdVector serialize  tmpStr size = %d", tmpStr.size());
            LOG(INFO, "blockIdVector serialize  tmpStr  = %s", tmpStr.data());
            LOG(INFO, "blockIdVector serialize  strlen = %d", strlen(tmpRes));
            LOG(INFO, "blockIdVector serialize  sizeof = %d", sizeof(tmpRes));
            int pid = getpid();

            int length = 0;
            char *res = (char *) malloc(4 + 1 + strlen(tmpRes) + 4);

            int *tmpInt = (int *) (res + length);
            *tmpInt = strlen(tmpRes);
            length + length + 4;

            char *tmpChar = (res + length);
            *tmpChar = acquireNewBlock & 0x0F;
            length = length + 1;

            memcpy(tmpChar + length, tmpRes, strlen(tmpRes));
            length = length + strlen(tmpRes);

            tmpInt = (int *) (res + length);
            *tmpInt = pid;
            length = length + 4;


            return res;
        }

         **/


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

            PutFixed32(&res, tmpStr.size());

            res.append(1, recordType & 0x0F);

            res.append(tmpStr.data(), tmpStr.size());
            LOG(INFO, "3, LogFormat res size = %d", res.size());
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
    }
}


//header
/**
-----------------------------------------------------
|   size  of data  | type                           |
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
-----------------------------------------------------------------------------------------------------------------
|total size| filename size| file name  | lastBucket| endOffsetOfBucket| num. of blocks| block 1 | block 2 | ....|
-----------------------------------------------------------------------------------------------------------------
|          |  10          | "file-test"|   6       |         45062    |   3           |     1   |   5     |    6|
-----------------------------------------------------------------------------------------------------------------
|          |  4 byte      | "file-test"|   4 byte  |         8 byte   |   4 byte      |  4 byte | 4 byte  |4 byte|
**/
