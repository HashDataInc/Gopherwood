//
// Created by houliang on 12/11/17.
//

#include "FileStatus.h"

namespace Gopherwood {
    namespace Internal {

    }

    /**
    -------------------------------------------------------------------------------------------------------------------
    |size of total data| type                           | num. of blocks| block 1 | block 2 | .... |endOffsetOfBucket |
    -------------------------------------------------------------------------------------------------------------------
    |   4536           |RecordType::closeFile           |   3           |     1   |   5     |    6 |45062             |
    -------------------------------------------------------------------------------------------------------------------
    |   4 byte         | 1 byte                         |   4 byte      |  4 byte | 4 byte  |4 byte|8 byte            |
    **/
    FileStatus *FileStatus::deSerializeFileStatus(char *res) {
        int length = 0;

        int totalLength = *((int *) (res + length));
        length += 4;
//            LOG(Gopherwood::Internal::INFO, "deSerializeFileStatus totalLength= %d", totalLength);

        int sizeOfFileName = *((int *) (res + length));
        char fileName[sizeOfFileName];


        length += 4;
//            LOG(Gopherwood::Internal::INFO, "deSerializeFileStatus sizeOfFileName= %d", sizeOfFileName);


        memcpy(fileName, res + length, sizeOfFileName);
//            for (int i = 0; i < sizeOfFileName; i++) {
//                LOG(Gopherwood::Internal::INFO, "deSerializeFileStatus (*(res + i))= %c", (*((res + length) + i)));
////                fileName[i] = (*((res + length) + i));
//                LOG(Gopherwood::Internal::INFO, "deSerializeFileStatus fileName[i]= %c", fileName[i]);
//            }
        length += sizeOfFileName;


        int lastBucket = *((int *) (res + length));
        length += 4;

        int64_t endOffsetOfBucket = *((int64_t *) (res + length));
        length += 8;

        int numOfBlocks = *((int *) (res + length));

        std::vector<int32_t> blockIdVector;

        for (int i = 0; i < numOfBlocks; i++) {
            length += 4;
            blockIdVector.push_back(*((int *) (res + length)));
        }

        FileStatus *status = new FileStatus();
        status->setEndOffsetOfBucket(endOffsetOfBucket);
        status->setFileName(fileName);
        status->setBlockIdVector(blockIdVector);
        status->setLastBucket(lastBucket);

        return status;

    }


}