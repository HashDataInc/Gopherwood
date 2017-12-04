//
// Created by root on 11/17/17.
//

#ifndef _GOPHERWOOD_CORE_FILESTATUS_H_
#define _GOPHERWOOD_CORE_FILESTATUS_H_


#include <vector>
#include <cstdint>
#include <string>

namespace Gopherwood {

    using namespace std;

    class FileStatus {

    public:

        FileStatus(){

        }

        ~FileStatus(){

        }

        const vector<int32_t> &getBlockIdVector() const {
            return blockIdVector;
        }

        const string &getFileName() const {
            return fileName;
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

        void setFileName(const string &fileName) {
            FileStatus::fileName = fileName;
        }

        void setLastBucket(int32_t lastBucket) {
            FileStatus::lastBucket = lastBucket;
        }

        void setEndOffsetOfBucket(int64_t endOffsetOfBucket) {
            FileStatus::endOffsetOfBucket = endOffsetOfBucket;
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


}


#endif //_GOPHERWOOD_CORE_FILESTATUS_H_
