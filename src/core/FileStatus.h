//
// Created by root on 11/17/17.
//

#ifndef _GOPHERWOOD_CORE_FILESTATUS_H_
#define _GOPHERWOOD_CORE_FILESTATUS_H_


#include <vector>
#include <cstdint>
#include <string>
#include <ostream>
#include <cstring>
#include <iostream>
#include "Logger.h"

#include "../common/Logger.h"

namespace Gopherwood {

    using namespace std;

    class FileStatus {

    public:

        FileStatus() {

        }

        ~FileStatus() {

        }

        const vector<int32_t> &getBlockIdVector() const {
            return blockIdVector;
        }

        int32_t getLastBucket() const {
            return lastBucket;
        }

        int64_t getEndOffsetOfBucket() const {
            return endOffsetOfBucket;
        }

        void setBlockIdVector(const vector<int32_t> &blockIdVector) {
            FileStatus::blockIdVector = blockIdVector;
        }


        void setLastBucket(int32_t lastBucket) {
            FileStatus::lastBucket = lastBucket;
        }

        void setEndOffsetOfBucket(int64_t endOffsetOfBucket) {
            FileStatus::endOffsetOfBucket = endOffsetOfBucket;
        }

        string getFileName() const {
            return fileName;
        }

        void setFileName(char *fileName) {
//            LOG(Gopherwood::Internal::INFO, "setFileName ,fileName = %s", fileName);
            FileStatus::fileName = fileName;
//            LOG(Gopherwood::Internal::INFO, "setFileName ,FileStatus::fileName = % s", FileStatus::fileName.data());
        }

        char* serializeFileStatus() {
            int32_t totalLength =
                    4/**filename size**/+ fileName.size() + 4/**lastBucket**/+ 8/**endOffsetOfBucket**/+
                    4/**num. of blocks**/+ 4 * blockIdVector.size();

//
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

        FileStatus *deSerializeFileStatus(char *res) {
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

            vector<int32_t> blockIdVector;

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
//
//        bool isExist() const {
//            return exist;
//        }

//        void setExist(bool exist) {
//            FileStatus::exist = exist;
//        }

    private:
        vector<int32_t> blockIdVector;//the block id's list that the file contains;
        string fileName;//the file's name;
        int32_t lastBucket = 0; // the last bucket that contains the real data;
        int64_t endOffsetOfBucket = 0; // the end offset of the bucket;
//        bool exist = false;
    };

    /**
    -----------------------------------------------------------------------------------------------------------------
    |total size| filename size| file name  | lastBucket| endOffsetOfBucket| num. of blocks| block 1 | block 2 | ....|
    -----------------------------------------------------------------------------------------------------------------
    |          |  10          | "file-test"|   6       |         45062    |   3           |     1   |   5     |    6|
    -----------------------------------------------------------------------------------------------------------------
    |          |  4 byte      | "file-test"|   4 byte  |         8 byte   |   4 byte      |  4 byte | 4 byte  |4 byte|
    **/



}


#endif //_GOPHERWOOD_CORE_FILESTATUS_H_
