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

        char *serializeFileStatus();

        FileStatus *deSerializeFileStatus(char *res);



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
