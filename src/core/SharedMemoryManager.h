//
// Created by root on 11/27/17.
//

#ifndef _GOPHERWOOD_CORE_SHAREDMEMORYMANAGER_H_
#define _GOPHERWOOD_CORE_SHAREDMEMORYMANAGER_H_

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/sem.h>
#include <memory>
#include <cstdint>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <sys/shm.h>
#include <iostream>
#include <cstring>
#include <string>
#include <cstdlib>
#include <fstream>
#include <cstddef>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include "Logger.h"
#include "FSConfig.h"
#include "LogFormat.h"

namespace Gopherwood {
    namespace Internal {
        using namespace std;
        using namespace boost::interprocess;

        class SharedMemoryManager {

        public:

            struct smBucketStruct {
                char type;
                int32_t blockIndex;
                char fileName[256];
            };


            SharedMemoryManager();

            ~SharedMemoryManager();


            /**
           * check whether the shared memory exist or not
           * @return 1 if exist, -1 otherwise
           */
            int32_t checkSharedMemory();

            void getSharedMemoryID();

            /**
             *  create the shared memory
             * @return the key of the shared memory
             */
            void createSharedMemory();


            /**
             * delete the shared memory
             * @return
             */
            int32_t deleteSharedMemory();


            void openSMBucket();

            void closeSMBucket();

            std::vector<int> acquireNewBlock(char *fileName);

            void inactiveBlock(int blockID, int blockIndex);

            void activeBlock(int blockID);

            void releaseBlock(int blockID);

            void evictBlock(int blockID, char *fileName);


            void closeSMFile();

            std::string getFileNameAccordingBlockID(int blockID);

            int getBlockIDIndex(int blockID);

            std::vector<int> getBlocksWhichTypeEqual2(int count);

        private:
            int32_t sharedMemoryFd = -1;// the shared memory file descriptor
            int32_t sharedMemoryID;// the shared memory id, when create a new shared memory, it will return a sharedMemoryID
//            char *smBucketInfo;
            std::shared_ptr<shared_memory_object> shmPtr;
            std::shared_ptr<mapped_region> regionPtr;

            union semun {
                int val;
                struct semid_ds *buf;
                unsigned short *arry;
            };

            int semaphoreID = 0;
            int TOTAL_SHARED_MEMORY_LENGTH = 1 + NUMBER_OF_BLOCKS * sizeof(smBucketStruct);

        private:
            void bitSet(char *p_data, int position, int flag);

            bool checkSharedMemoryInFile();

            int createSemaphore();

            int setSemaphoreValue();

            void delSemaphoreValue();

            int semaphoreP();

            int semaphoreV();

            int checkSemaphore();

            bool checkAndSetSMOne();

            void checkAndSetSMZero();

            char *generateStr(int length);

            bool checkBlockIDIsLegal(int blockID);

            void printSMStatus();


        };

    }


}


#endif //_GOPHERWOOD_CORE_SHAREDMEMORYMANAGER_H_
