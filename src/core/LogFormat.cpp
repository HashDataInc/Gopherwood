//
// Created by root on 12/12/17.
//

#include <iostream>
#include "LogFormat.h"
#include "Logger.h"
#include <unistd.h>
#include <string.h>
#include "../common/Logger.h"

namespace Gopherwood {
    namespace Internal {

        LogFormat::LogFormat() {

        }

        LogFormat::~LogFormat() {

        }


        char *LogFormat::serializeBlockIDVector(const std::vector<int32_t> &blockIdVector) {
            int totalLength = 4/**num. of blocks**/+ 4 * (int) blockIdVector.size();

            char *res = (char *) (malloc(totalLength));

            int length = 0;

            int *tmp = (int *) (res + length);

            *tmp = (int) blockIdVector.size();
            length += 4;

            for (int i = 0; i < blockIdVector.size(); i++) {
                tmp = (int *) (res + length);
                *tmp = blockIdVector[i];
                LOG(INFO, "*********blockIdVector= %d", *((int *) (res + length)));
                length += 4;
            }
            return res;
        }


        char *LogFormat::serializeAcquireNewBlock(const std::vector<int32_t> &blockIdVector) {
            char *tmpRes = serializeBlockIDVector(blockIdVector);
            LOG(INFO, "blockIdVector serialize  size = %d", strlen(tmpRes));
            int pid = getpid();

            int length = 0;
            char *res = (char *) malloc(4 + 1 + strlen(tmpRes) + 4);

            int *tmpInt = (int *) (res + length);
            *tmpInt = strlen(tmpRes);
            length + length + 4;

            char *tmpChar = (res + length);
            *tmpChar =acquireNewBlock & 0x0F;
            length = length + 1;

            memcpy(tmpChar + length, tmpRes, strlen(tmpRes));
            length = length + strlen(tmpRes);

            tmpInt = (int *) (res + length);
            *tmpInt = pid;
            length = length + 4;


            return res;
        }


        char *LogFormat::serializeLog(RecordType type, const std::vector<int32_t> &blockIdVector) {
            LOG(INFO, "LogFormat::serializeLog, and the RecordType =  %d, blockIdVector size = ", type,
                blockIdVector.size());
            switch (type) {
                case acquireNewBlock:
                    char *res = serializeAcquireNewBlock(blockIdVector);
                    return res;
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
