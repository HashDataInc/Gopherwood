//
// Created by houliang on 12/11/17.
//

#include "FileStatus.h"

namespace Gopherwood {
    namespace Internal {

    }

    char *FileStatus::serializeFileStatus() {
        int32_t totalLength =
                4/**filename size**/+ fileName.size() + 4/**lastBucket**/+ 8/**endOffsetOfBucket**/+
                4/**num. of blocks**/+ 4 * blockIdVector.size();


        LOG(Gopherwood::Internal::INFO, "fileName = %s", fileName.data());
        LOG(Gopherwood::Internal::INFO, "lastBucket = %d", lastBucket);
        LOG(Gopherwood::Internal::INFO, "endOffsetOfBucket = %d", endOffsetOfBucket);
        LOG(Gopherwood::Internal::INFO, "totalLength = %d", totalLength);
        LOG(Gopherwood::Internal::INFO, "blockIdVector.size() = %d", blockIdVector.size());

        char *res = (char *) (malloc(4 + totalLength));

//            char res[4 + totalLength];
        int length = 0;

        int *tmp = (int *) (res + length);
        *tmp = totalLength;
//            LOG(Gopherwood::Internal::INFO, "*********serializeFileStatus totalLength= %d", *((int *) (res + length)));
        length += 4;


        tmp = (int *) (res + length);
        *tmp = fileName.size();
//            LOG(Gopherwood::Internal::INFO, "*********strlen(fileName)= %d", *((int *) (res + length)));
        length += 4;


        memcpy(res + length, fileName.data(), fileName.size());
//            LOG(Gopherwood::Internal::INFO, "*********fileName= %s", (res + length));
        length += fileName.size();

        tmp = (int *) (res + length);
        *tmp = lastBucket;
//            LOG(Gopherwood::Internal::INFO, "*********lastBucket= %d", *((int *) (res + length)));
        length += 4;

        int64_t *tmp64 = (int64_t *) (res + length);
        *tmp64 = endOffsetOfBucket;
//            LOG(Gopherwood::Internal::INFO, "*********endOffsetOfBucket= %d", *((int64_t *) (res + length)));
        length += 8;

        tmp = (int *) (res + length);
        *tmp = blockIdVector.size();
        length += 4;

        for (int i = 0; i < blockIdVector.size(); i++) {
            tmp = (int *) (res + length);
            *tmp = blockIdVector[i];
//                LOG(Gopherwood::Internal::INFO, "*********blockIdVector= %d", *((int *) (res + length)));
            length += 4;
        }


        /**
        length = 0;
        int tmpTotalLength = *((int *) (res + length));

        LOG(Gopherwood::Internal::INFO, "serializeFileStatus totalLength= %d", tmpTotalLength);
        length += 4;

        LOG(Gopherwood::Internal::INFO, "serializeFileStatus sizeof(fileName) = %d", *((int *) (res + length)));
        length += 4;
        for (int i = 0; i < fileName.size(); i++) {
            LOG(Gopherwood::Internal::INFO, "serializeFileStatus fileName = %c", (*(res + length + i)));
        }


        length += fileName.size();
        LOG(Gopherwood::Internal::INFO, "serializeFileStatus lastBucket = %d", *((int *) (res + length)));

        length += 4;
        LOG(Gopherwood::Internal::INFO, "serializeFileStatus endOffsetOfBucket = %d",
            *((int64_t *) (res + length)));

        length += 8;
        LOG(Gopherwood::Internal::INFO, "serializeFileStatus numOfBlocks = %d", *((int *) (res + length)));

        for (int i = 0; i < blockIdVector.size(); i++) {
            length += 4;
            LOG(Gopherwood::Internal::INFO, "serializeFileStatus blockIdVector = %d", *((int *) (res + length)));
        }
**/

        return res;
    }


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