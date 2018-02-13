//
// Created by houliang on 11/17/17.
//

#ifndef _GOPHERWOOD_CORE_FILESTATUS_H_
#define _GOPHERWOOD_CORE_FILESTATUS_H_


#include <vector>
#include <cstdint>
#include <string>
#include <ostream>
#include <cstring>
#include <iostream>
#include <memory>

#include "Logger.h"
#include "FSConfig.h"


#include "../common/LRUCache.cpp"
#include "../common/Logger.h"

namespace Gopherwood {

    using namespace std;

    class FileStatus {

    public:

        typedef enum AccessFileType {

            sequenceType = 0,

            randomType = 1,

            hintRandomType = 2,

            hintSequenceType = 3,
        } AccessFileType;


        FileStatus() {
            LRUCache<int, int> *tmpLruCache = new LRUCache<int, int>(Gopherwood::Internal::MAX_QUOTA_SIZE);
            std::shared_ptr<LRUCache<int, int> > tmpPtr(tmpLruCache);
            lruCache = tmpPtr;
        }

        ~FileStatus() {

        }


        shared_ptr<LRUCache<int, int>> getLruCache() const {
            return lruCache;
        }

        void setLruCache(const shared_ptr<LRUCache<int, int>> lruCache) {
            FileStatus::lruCache = lruCache;
        }


        vector<int32_t> getBlockIdVector() {
            return blockIdVector;
        }

//        vector<int32_t> getPingIDVector() {
//            return pingIDVector;
//        }

        int32_t getLastBucket() {
            return lastBucket;
        }

        int32_t getLastBucketIndex() {
            return lastBucketIndex;
        }

        int64_t getEndOffsetOfBucket() {
            return endOffsetOfBucket;
        }

        void setBlockIdVector(vector<int32_t> blockIdVector) {
            FileStatus::blockIdVector = blockIdVector;
        }

//        void setPingIDVector(vector<int32_t> pingIDVector) {
//            FileStatus::pingIDVector = pingIDVector;
//        }


        void setLastBucket(int32_t lastBucket) {
            FileStatus::lastBucket = lastBucket;
        }

        void setLastBucketIndex(int32_t lastBucketIndex) {
            FileStatus::lastBucketIndex = lastBucketIndex;
        }


        void setEndOffsetOfBucket(int64_t endOffsetOfBucket) {
            FileStatus::endOffsetOfBucket = endOffsetOfBucket;
        }

        string getFileName() {
            return fileName;
        }

        int64_t getLogOffset() {
            return logOffset;
        }

        void setLogOffset(int64_t logOffset) {
            FileStatus::logOffset = logOffset;
        }


        void setFileName(char *fileName) {
//            LOG(Gopherwood::Internal::INFO, "setFileName ,fileName = %s", fileName);
            FileStatus::fileName = fileName;
//            LOG(Gopherwood::Internal::INFO, "setFileName ,FileStatus::fileName = % s", FileStatus::fileName.data());
        }

        FileStatus *deSerializeFileStatus(char *res);

        AccessFileType getAccessFileType() const {
            return accessFileType;
        }

        void setAccessFileType(AccessFileType accessFileType) {
            FileStatus::accessFileType = accessFileType;
        }

    private:
        vector<int32_t> blockIdVector;//the block id's list that the file contains;
        string fileName;//the file's name;
        int32_t lastBucket = 0; // the last bucket that contains the real data;
        int32_t lastBucketIndex = 0;

        int64_t endOffsetOfBucket = 0; // the end offset of the bucket;
        int64_t logOffset = 0; // the log offset that have been chased
//        vector<int32_t> pingIDVector; // the block id's list which shared memory type ='1', and can not be
        std::shared_ptr<LRUCache<int, int>> lruCache;

        AccessFileType accessFileType;

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
