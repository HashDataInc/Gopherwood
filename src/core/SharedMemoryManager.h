/********************************************************************
 * 2017 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        char isKick;
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

    std::unordered_map<int, std::string> getBlocksWhichTypeEqual2(int count);

    void printSMStatus();

    char getBlockType(int blockID);

    bool checkFileNameAndType(int blockID, string fileName);

    bool checkFileNameAndTypeAndSetKick(int blockID, string fileName);

    std::string getBlockStatus(int blockID);

    void getLock();

    void releaseLock();

private:
    int32_t sharedMemoryFd = -1; // the shared memory file descriptor
    int32_t sharedMemoryID; // the shared memory id, when create a new shared memory, it will return a sharedMemoryID
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

};

}

}

#endif //_GOPHERWOOD_CORE_SHAREDMEMORYMANAGER_H_
