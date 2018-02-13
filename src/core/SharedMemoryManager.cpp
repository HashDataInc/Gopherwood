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
#include <sstream>
#include "SharedMemoryManager.h"
#include "../util/Coding.h"

namespace Gopherwood {

using namespace boost::interprocess;
namespace Internal {

SharedMemoryManager::SharedMemoryManager() {

}

SharedMemoryManager::~SharedMemoryManager() {

}

int SharedMemoryManager::checkSharedMemory() {
    //TODO 1. check the shared memory in memory exist or not?
    //2. check the file exist or not?
    checkSharedMemoryInFile();
    return 0;
}

bool SharedMemoryManager::checkSharedMemoryInFile() {
    int flags = O_RDONLY;
    int sharedMemoryFd = open(SHARED_MEMORY_PATH_FILE_NAME, flags, 0777);
    if (sharedMemoryFd < 0) {
        LOG(INFO, "the shared memory file not exist, so create one");
        createSharedMemory();
    } else {
        LOG(INFO, "the shared memory file exist");
    }
    return true;
}

/**
 * create the shared memory
 */
void SharedMemoryManager::createSharedMemory() {
    if (!semaphoreP()) {
        LOG(LOG_ERROR, "can not acquire the semaphore, create SharedMemory failure");
    }

    file_mapping::remove(SHARED_MEMORY_PATH_FILE_NAME);
    std::filebuf fbuf;
    fbuf.open(
            SHARED_MEMORY_PATH_FILE_NAME,
            std::ios_base::in | std::ios_base::out | std::ios_base::trunc | std::ios_base::binary);

    //IMPORTANT,init the shared memory to '0', not 0
    for (int i = 0; i < TOTAL_SHARED_MEMORY_LENGTH; i++) {
        fbuf.sputc('0');
    }
    if (!semaphoreV()) {
        LOG(LOG_ERROR, "can not release the semaphore, create SharedMemory failure");
    }
}

int32_t SharedMemoryManager::deleteSharedMemory() {
    return 0;
}

void SharedMemoryManager::getSharedMemoryID() {

}

/**
 *
 * @return the SharedMemoryBucket object
 */
void SharedMemoryManager::openSMBucket() {
//            LOG(INFO, "come in the openSMBucket");
    file_mapping m_file(SHARED_MEMORY_PATH_FILE_NAME, read_write);

    std::shared_ptr < mapped_region > tmp(new mapped_region(m_file, read_write));
    regionPtr = tmp;

//            printSMStatus();
}

void SharedMemoryManager::printSMStatus() {
    char *mem = (char*) regionPtr->get_address();
    int length = 0;
    char sempType = *((char *) (mem + length));
    LOG(INFO, "semaphore flag = %c", sempType);
    length = length + 1;
    smBucketStruct *smb;

    while ((length < TOTAL_SHARED_MEMORY_LENGTH)) {
        smb = (smBucketStruct *) (mem + length);
        char type = smb->type;
        int blockIndex = smb->blockIndex;
        char isKick = smb->isKick;
        char fileName[256];
        memcpy(fileName, smb->fileName, sizeof(fileName));
        if (type != '0') {
            LOG(
                    INFO,
                    "type = %c, blockIndex = %d, fileName=%s, isKick=%c",
                    type,
                    blockIndex,
                    fileName,
                    isKick);
        } else {
            LOG(INFO, "type = %c, length = %d", type, length);
        }
        length += sizeof(smBucketStruct);
    }
}

void SharedMemoryManager::closeSMFile() {
    close(sharedMemoryID);
}

void SharedMemoryManager::closeSMBucket() {

}

bool SharedMemoryManager::checkAndSetSMOne() {
    char *mem = static_cast<char *>(regionPtr->get_address());
    if (*(mem) != '0') {
        LOG(LOG_ERROR, "shared memory is broken");
        return true;
    } else {
        *mem = '1';
    }
    return false;
}

void SharedMemoryManager::checkAndSetSMZero() {
    char *mem = static_cast<char *>(regionPtr->get_address());
    *mem = '0';
}

int SharedMemoryManager::getBlockIDIndex(int blockID) {

    getLock();

    char *mem = (char *) regionPtr->get_address();
    smBucketStruct *smb;
    int length = 1 + sizeof(smBucketStruct) * blockID;
    smb = (smBucketStruct *) (mem + length);
    int index = smb->blockIndex;

    releaseLock();
    return index;
}

std::vector<int> SharedMemoryManager::acquireNewBlock(char *fileName) {

//            LOG(INFO, "acquireNewBlock method");
    if (strlen(fileName) > FILENAME_MAX_LENGTH) {
        LOG(
                LOG_ERROR,
                "SharedMemoryManager::acquireNewBlock. fileName name size cannot be larger than %d, ",
                FILENAME_MAX_LENGTH);
    }

    getLock();

    //TODO FOT TEST*****************
    LOG(INFO, "SharedMemoryManager::acquireNewBlock before shared memory status");
    printSMStatus();

    std::vector<int> resVector;

    int i = 0;
    int length = 1;
    char *mem = (char*) regionPtr->get_address();
    smBucketStruct *smb;

    // 1. acquire new block lists
    while (length < TOTAL_SHARED_MEMORY_LENGTH) {
        smb = (smBucketStruct *) (mem + length);

        char type = smb->type;
        if (type == '0') {
            //1.write type
            smb->type = '1';

            //3. write the file name
            string strFileName;
            strFileName.append(fileName, strlen(fileName));
            strFileName.append(1, '\0');
            memcpy(smb->fileName, strFileName.data(), strFileName.size());

//                    LOG(INFO, "file size = %d", strlen(fileName));

            resVector.push_back(i);

            if (resVector.size() >= MIN_QUOTA_SIZE) {
                break;
            }
        }

        length = length + sizeof(smBucketStruct);
        i++;
    }

    releaseLock();

    if (length >= TOTAL_SHARED_MEMORY_LENGTH) {
        LOG(LOG_ERROR, "SharedMemoryManager::acquireNewBlock. no enough room for the ssd bucket");
    }

    LOG(INFO, "SharedMemoryManager::acquireNewBlock after shared memory status");
    printSMStatus();
    return resVector;
}

std::unordered_map<int, std::string> SharedMemoryManager::getBlocksWhichTypeEqual2(int count) {

    getLock();

    std::unordered_map<int, std::string> blockStatusMap;

    int i = 0;
    int length = 1;
    char *mem = (char*) regionPtr->get_address();
    smBucketStruct *smb;

    // 1. get the  new block lists which type = '2'
    while (length < TOTAL_SHARED_MEMORY_LENGTH) {
        smb = (smBucketStruct *) (mem + length);
        char type = smb->type;
        char isKick = smb->isKick;
        if (type == '2' && isKick != '1') {
            //2. set the isKick='1'
            smb->isKick = '1';

            // 3. construct the block status
            char *fileName = smb->fileName;
            int blockIndex = smb->blockIndex;

            string blockStatus;
            blockStatus.append(fileName, strlen(fileName));

            stringstream ss;
            ss << blockStatus << "-" << blockIndex << "-" << type << "-1";
            blockStatusMap[i] = ss.str();

            LOG(
                    INFO,
                    "SharedMemoryManager::getBlocksWhichTypeEqual2. the bloclIO =%d, and the block status =%s",
                    i,
                    ss.str().c_str());

            if (blockStatusMap.size() >= (unsigned int) count) {
                break;
            }
        }
        length = length + sizeof(smBucketStruct);
        i++;
    }
    releaseLock();
    if (length >= TOTAL_SHARED_MEMORY_LENGTH) {
        LOG(
                LOG_ERROR,
                "SharedMemoryManager::getBlocksWhichTypeEqual2. no enough room for the ssd bucket");
    }
    return blockStatusMap;
}

void SharedMemoryManager::activeBlock(int blockID) {
    if (!checkBlockIDIsLegal(blockID)) {
        return;
    }

    getLock();

    int length = 1;
    char *mem = (char*) regionPtr->get_address();
    length += sizeof(smBucketStruct) * blockID;
    smBucketStruct *smb = (smBucketStruct *) (mem + length);

    //1.change the type
    smb->type = '1';

    releaseLock();
    LOG(INFO, "SharedMemoryManager::activeBlock after shared memory status");
    printSMStatus();

}

void SharedMemoryManager::inactiveBlock(int blockID, int blockIndex) {
    if (!checkBlockIDIsLegal(blockID)) {
        return;
    }

    getLock();

    int length = 1;
    char *mem = (char*) regionPtr->get_address();
    length += sizeof(smBucketStruct) * blockID;
    smBucketStruct *smb = (smBucketStruct *) (mem + length);

    //1.change the type
    smb->type = '2';

    //2. set the isKick false
    smb->isKick = '0';

    //2. set the block index
    smb->blockIndex = blockIndex;

    if (length >= TOTAL_SHARED_MEMORY_LENGTH) {
        LOG(
                LOG_ERROR,
                "SharedMemoryManager::inactiveBlock. given block id = %d exceed the max size which is ",
                blockID);
    }

    releaseLock();

    printSMStatus();
}

void SharedMemoryManager::releaseBlock(int blockID) {

    if (!checkBlockIDIsLegal(blockID)) {
        return;
    }

    getLock();

    int length = 1;
    char *mem = (char*) regionPtr->get_address();
    length += sizeof(smBucketStruct) * blockID;
    smBucketStruct *smb = (smBucketStruct *) (mem + length);

    smb->type = '0';

    releaseLock();
    printSMStatus();
}

std::string SharedMemoryManager::getFileNameAccordingBlockID(int blockID) {
    if (!checkBlockIDIsLegal(blockID)) {
        return NULL;
    }

    getLock();

    int length = 1;
    char *mem = (char*) regionPtr->get_address();
    length += sizeof(smBucketStruct) * blockID;

    smBucketStruct *smb = (smBucketStruct *) (mem + length);

    char tmpName[256];

    memcpy(tmpName, smb->fileName, sizeof(smb->fileName));
//            LOG(INFO, " previous file name = %s", tmpName);

    string retVal;
    retVal.append(tmpName, sizeof(smb->fileName));
    releaseLock();
    return retVal;
}

// BUG-FIX. do not have to get lock, because the outside of the function have acquire the block
std::string SharedMemoryManager::getBlockStatus(int blockID) {
    if (!checkBlockIDIsLegal(blockID)) {
        LOG(LOG_ERROR, "SharedMemoryManager::getBlockStatus. the blockID=%d is not legal", blockID);
        return NULL;
    }

    int length = 1;
    char *mem = (char*) regionPtr->get_address();
    length += sizeof(smBucketStruct) * blockID;

    smBucketStruct *smb = (smBucketStruct *) (mem + length);

    // 3. construct the block status
    char *fileName = smb->fileName;
    int blockIndex = smb->blockIndex;
    char type = smb->type;
    char isKick = smb->isKick;

    string blockStatus;
    blockStatus.append(fileName, strlen(fileName));
    stringstream ss;
    ss << blockStatus << "-" << blockIndex << "-" << type << "-" << isKick;

    LOG(
            INFO,
            "SharedMemoryManager::getBlockStatus. the blockID=%d, and the block status=%s.",
            blockID,
            ss.str().c_str());

    return ss.str();
}

void SharedMemoryManager::getLock() {
    if (!semaphoreP()) {
        LOG(
                LOG_ERROR,
                "SharedMemoryManager::evictBlock. can not acquire the semaphore, acquireNewBlock failure");
    }
    if (checkAndSetSMOne()) {
        LOG(
                LOG_ERROR,
                "SharedMemoryManager::evictBlock. acquireNewBlock failed, shared memory is broken");
    }

}

void SharedMemoryManager::releaseLock() {
    checkAndSetSMZero();
    if (!semaphoreV()) {
        LOG(
                LOG_ERROR,
                "SharedMemoryManager::evictBlock. can not release the semaphore, acquireNewBlock failure");
    }

}

void SharedMemoryManager::evictBlock(int blockID, char *fileName) {
    if (!checkBlockIDIsLegal(blockID)) {
        return;
    }

    int length = 1;
    char *mem = (char*) regionPtr->get_address();
    length = length + sizeof(smBucketStruct) * blockID;
    smBucketStruct *smb = (smBucketStruct *) (mem + length);

    //1.set the type
    smb->type = '1';

    //2. write the file length

    //3.change the fileName
    string strFileName;
    strFileName.append(fileName, strlen(fileName));
    strFileName.append(1, '\0');
    memcpy(smb->fileName, strFileName.data(), strFileName.size());

    printSMStatus();
}

bool SharedMemoryManager::checkBlockIDIsLegal(int blockID) {
    if (blockID > NUMBER_OF_BLOCKS - 1) {
        LOG(
                LOG_ERROR,
                "SharedMemoryManager::evictBlock. given block id %d exceed the max size which is %d ",
                blockID,
                NUMBER_OF_BLOCKS - 1);
        return false;
    }
    return true;
}

char SharedMemoryManager::getBlockType(int blockID) {
    getLock();

    int length = 1;
    char *mem = (char*) regionPtr->get_address();
    length = length + sizeof(smBucketStruct) * blockID;
    smBucketStruct *smb = (smBucketStruct *) (mem + length);

    char type = smb->type;

    releaseLock();
    return type;
}

bool SharedMemoryManager::checkFileNameAndType(int blockID, string fileName) {
    if (!checkBlockIDIsLegal(blockID)) {
        return NULL;
    }

    getLock();

    int length = 1;
    char *mem = (char*) regionPtr->get_address();
    length = length + sizeof(smBucketStruct) * blockID;
    smBucketStruct *smb = (smBucketStruct *) (mem + length);

    //1. get type
    char type = smb->type;

    //2. get fileName
    char tmpName[256];
    memcpy(tmpName, smb->fileName, sizeof(smb->fileName));
    std::string previousFileName;
    previousFileName.append(tmpName, sizeof(smb->fileName));

    releaseLock();
    if ((std::strcmp(fileName.data(), previousFileName.data()) == 0) && (type == '2')) {
        return true;
    }
    return false;
}

bool SharedMemoryManager::checkFileNameAndTypeAndSetKick(int blockID, string fileName) {
    LOG(INFO, "SharedMemoryManager::checkFileNameAndTypeAndSetKick. come in here");
    if (!checkBlockIDIsLegal(blockID)) {
        return NULL;
    }

    getLock();

    int length = 1;
    char *mem = (char*) regionPtr->get_address();
    length = length + sizeof(smBucketStruct) * blockID;
    smBucketStruct *smb = (smBucketStruct *) (mem + length);

    //1. get type
    char type = smb->type;

    // 2. get kick type;
    //char isKick = smb->isKick;

    //3. get fileName
    char tmpName[256];
    memcpy(tmpName, smb->fileName, sizeof(smb->fileName));
    std::string previousFileName;
    previousFileName.append(tmpName, sizeof(smb->fileName));

    //BUG-FIX. before, we want to only check the type == '2' or not. do not check the  isKick parameter,
    // just because we want to use this block(pin this block) even if the block is evicting to OSS.
    //however, it will cause error kile below:
    /**
     process A                                           process B
     t0                                                             acquire new block, (type,isKick):(2,0)->(2,1);
     t1         find and pin the block. type:(2)->type:(1);
     t2         inactive the block. type:(1)->(type,isKick):(2,0);
     t3         acquire the block. (type,isKick):(2,0)->(2,1);
     t4
     t5                                                             check the sm block status is right. (type,isKick):(2,0)->(2,1);
     t6                                                             commit the change;type,isKick):(1,*)
     t7         find error;
     **/

    // Solution. still do not check the isKick, we solve the problem by recheck the block status(fileName, index,type)
    // in the FileSystemImpl::evictBlock() method.
    if ((std::strcmp(fileName.data(), previousFileName.data()) == 0) && (type == '2')) {
        smb->type = '1';
        releaseLock();
        return true;
    } else {
        releaseLock();
        return false;
    }
}

// ******************* semaphore **************************

int SharedMemoryManager::checkSemaphore() {
    key_t key = ftok(SHARED_MEMORY_PATH_FILE_NAME, 1);
    int tmpSemID = semget(key, 1, 0666 | IPC_CREAT | IPC_EXCL);

    //this means the semaphore already exist, get the semaphoreID
    if (tmpSemID == -1) {
        //get the semaphore
//                LOG(INFO, "semaphore already exist, acquire the semaphore");
        createSemaphore();
    } else {
        //create the semaphore
//                LOG(INFO, "semaphore do not exist, create a new one and init it");
        createSemaphore();
        //init the semaphore
        setSemaphoreValue();
    }
    return tmpSemID;
}

int SharedMemoryManager::createSemaphore() {
    key_t key = ftok(SHARED_MEMORY_PATH_FILE_NAME, 1);
    semaphoreID = semget(key, 1, 0666 | IPC_CREAT);
    return semaphoreID;
}

int SharedMemoryManager::setSemaphoreValue() {
//            LOG(INFO, "setSemaphoreValue method");

    //init semaphore
    union semun sem_union;

    sem_union.val = 1;
    if (semctl(semaphoreID, 0, SETVAL, sem_union) == -1)
        return 0;
    return 1;
}

void SharedMemoryManager::delSemaphoreValue() {
    union semun sem_union;

    if (semctl(semaphoreID, 0, IPC_RMID, sem_union) == -1) {
        LOG(LOG_ERROR, "Failed to delete semaphore");
    }
}

int SharedMemoryManager::semaphoreP() {
//            LOG(INFO, "semaphoreP method");

    checkSemaphore();

    //P（sv）
    struct sembuf sem_b;
    sem_b.sem_num = 0;
    sem_b.sem_op = -1;            //P()
    sem_b.sem_flg = SEM_UNDO;
    if (semop(semaphoreID, &sem_b, 1) == -1) {
        LOG(LOG_ERROR, "semaphoreP failed");
        return 0;
    }
    return 1;
}

int SharedMemoryManager::semaphoreV() {
//            LOG(INFO, "semaphoreV method");
    //V（sv）
    struct sembuf sem_b;
    sem_b.sem_num = 0;
    sem_b.sem_op = 1;            //V()
    sem_b.sem_flg = SEM_UNDO;
    if (semop(semaphoreID, &sem_b, 1) == -1) {
        LOG(LOG_ERROR, "semaphoreV failed");
        return 0;
    }
    return 1;
}

// ******************* semaphore **************************

}
}
