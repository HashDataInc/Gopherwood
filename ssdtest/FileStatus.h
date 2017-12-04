//
// Created by root on 11/17/17.
//

#ifndef GOPHERWOOD_FILESTATUS_H
#define GOPHERWOOD_FILESTATUS_H

#include <vector>
#include "stdint.h"
#include <string>
#include <iostream>
using namespace std;
class FileStatus {

public:

	FileStatus() {

	}

	~FileStatus() {

	}

	vector<int32_t> &getBlockIdVector() {
		return blockIdVector;
	}

	string &getFileName() {
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
	std::vector<int32_t> blockIdVector; //the block id's list that the file contains;
	std::string fileName; //the file's name;
	int32_t lastBucket; // the last bucket that contains the real data;
	int64_t endOffsetOfBucket; // the end offset of the bucket;
//        bool exist = false;
};

#endif //GOPHERWOOD_FILESTATUS_H
