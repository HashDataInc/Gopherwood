//
// Created by neuyilan@163.com on 11/17/17.
//

#ifndef _GOPHERWOOD_CORE_FILESYSTEMIMPL_H_
#define _GOPHERWOOD_CORE_FILESYSTEMIMPL_H_

#include <unordered_map>
#include <cstdint>
#include <unistd.h>
#include <bitset>
#include <fcntl.h>
#include <sys/shm.h>
#include <iostream>

#include "FileSystemInter.h"
#include "FileStatus.h"
#include "FSConfig.h"
#include "Exception.h"
#include "Logger.h"
#include "SharedMemoryManager.h"
#include "QingStoreReadWrite.h"

namespace Gopherwood {
    namespace Internal {

        class InputStreamInter;

        class OutputStreamInter;

        class FileSystemImpl : public FileSystemInter {

        public:

            FileSystemImpl();

            /**
            * Destroy a FileSystemBase instance
            */
            ~FileSystemImpl();


            /**
             * check whether the ssd file exist or not
             * @return 1 if exist, -1 otherwise
             */
            int32_t checkSSDFile();

//            int32_t checkSharedMemoryFile();

            void changePingBlockActive(int blockID);

            void rebuildFileStatusFromLog(char *fileName);

            void catchUpFileStatusFromLog(char *fileName);

            bool checkBlockIDWithFileName(int blockID, string fileName);

            void writeDataFromOSS2Bucket(int64_t ossindex, string fileName);

            int getOneBlockForWrite(int ossindex, string fileName);

            void deleteBlockFromOSS(int64_t ossindex, string fileName);


            // TODO JUST FOT TEST
            void readTotalDataFromFile(std::shared_ptr<FileStatus> fileStatus);

            void readTotalRandomDataFromFile(std::shared_ptr<FileStatus> fileStatus);

            void readTotalRandomDataFromVerifyFile(vector<int32_t> randomIndexVector,
                                                   std::shared_ptr<FileStatus> fileStatus);

            void writeCharStrUtil(string fileName, char *buf, int64_t size);

            void writeIntArrayUtil(string fileName, vector<int32_t> randomVector);

            void
            readDataFromFileAccordingToBlockID(int blockID, std::shared_ptr<FileStatus> fileStatus, string suffixName);

            int getRandomIntValue(int start, int end);
            // TODO JUST FOT TEST


            void getLock();

            void releaseLock();

            void deleteFile(char *fileName);

            int destroyFileSystem();

        private:

            int32_t bucketFd = -1;// the bucket file descriptor

            unordered_map<string, std::shared_ptr<FileStatus>> fileStatusMap; // the file status' map

            std::shared_ptr<SharedMemoryManager> sharedMemoryManager;
            std::shared_ptr<LogFormat> logFormat;
            std::shared_ptr<QingStoreReadWrite> qsReadWrite;

        private:

            void getSharedMemoryID();

            bool checkFileExist(char *fileName);

            void createFile(char *fileName);

            void acquireNewBlock(char *fileName);

            void inactiveBlock(char *fileName, const std::vector<int32_t> &blockIdVector);

            void releaseBlock(char *fileName, const std::vector<int32_t> &blockIdVector);

            void deleteBlockFromSSD(char *fileName, const std::vector<int32_t> &blockIdVector);

            std::vector<int> evictBlock(char *fileName, std::unordered_map<int, std::string> blockStatusMap);

            int64_t getTheEOFOffset(const char *fileName);

            int64_t readDataFromBucket(char *buf, int32_t size);

            void writeDataToBucket(char *buf, int64_t size);

            void checkBucketFileOpen();

            int32_t fsSeek(int64_t offset, int whence);

            void closeBucketFile();

            void closeFile(char *fileName);

            void stopSystem();

            void releaseOrInactiveLRUCache(char *fileName);

            std::shared_ptr<FileStatus> getFileStatus(const char *fileName);

            void persistentFileLog(char *fileName);

            //TODO JUST FOR TEST
            void readCloseFileStatus(char *fileName);


            void writeFileStatusToLog(char *fileName, std::string data);

            char *getFilePath(char *fileName);

            void writeDate2OSS(char *fileNameInOSS, int blockID);

            int getIndexAccordingBlockID(char *fileName, int blockID);

            std::string constructFileKey(std::string, int index);

            void checkAndAddBlockID(char *fileName, std::vector<int32_t> blockIDVector);

            void checkAndAddPingBlockID(char *fileName, std::vector<int32_t> blockIDVector);

            std::vector<int32_t> deleteVector(std::vector<int32_t> previousVector, std::vector<int32_t> toDeleteVector);
        };


    }
}

#endif //_GOPHERWOOD_CORE_FILESYSTEMIMPL_H_