//
// Created by root on 11/17/17.
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

namespace Gopherwood {
    namespace Internal {

        class InputStreamInter;

        class OutputStreamInter;

        class FileSystemImpl : public FileSystemInter {

        public:

            FileSystemImpl(char *fileName);

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

            std::unordered_map<string, std::shared_ptr<FileStatus>> rebuildFileStatusFromLog(char *fileName);

            std::unordered_map<string, std::shared_ptr<FileStatus>> catchUpFileStatusFromLog(int64_t logOffset);


        private:

            int32_t bucketFd = -1;// the bucket file descriptor

            unordered_map<string, std::shared_ptr<FileStatus>> fileStatusMap; // the file status' map

            std::shared_ptr<SharedMemoryManager> sharedMemoryManager;

//            static const int32_t BIT_MAP_SIZE = 40;
//            uint64_t filesize_;
//            string fileName;

        private:

            void getSharedMemoryID();

            bool checkFileExist(char *fileName);

            void createFile(char *fileName);

            void acquireNewBlock(char *fileName);

            int64_t getTheEOFOffset(const char *fileName);

            std::shared_ptr<FileStatus> getFileStatus(const char *fileName);

            int32_t readDataFromBucket(char *buf, int32_t size);

            void writeDataToBucket(char *buf, int64_t size);

            void checkBucketFileOpen();

            int32_t fsSeek(int64_t offset, int whence);

            void closeBucketFile();

            void closeFile(char *fileName);

            void stopSystem();

            std::shared_ptr<FileStatus> getFileStatus(char *fileName);

            void persistentFileLog(char * fileName);

        };


    }
}

#endif //_GOPHERWOOD_CORE_FILESYSTEMIMPL_H_