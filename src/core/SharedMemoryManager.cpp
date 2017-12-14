//
// Created by root on 11/27/17.
//


#include "SharedMemoryManager.h"
#include "../common/Logger.h"

namespace Gopherwood {

    using namespace boost::interprocess;
    namespace Internal {


        SharedMemoryManager::SharedMemoryManager() {

        }

        SharedMemoryManager::~SharedMemoryManager() {

        }


        int SharedMemoryManager::checkSharedMemory() {
            //TODO 1. check the shared memory in memry exist or not?
            //2. check the file exist or not?
            checkSharedMemoryInFile();

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
            fbuf.open(SHARED_MEMORY_PATH_FILE_NAME, std::ios_base::in | std::ios_base::out
                                                    | std::ios_base::trunc | std::ios_base::binary);
//            fbuf.pubseekoff(SM_FILE_SIZE - 1, std::ios_base::beg);

            //IMPORTANT,init the shared memory to '0', not 0
            for (int i = 0; i < SM_FILE_SIZE; i++) {
                fbuf.sputc('0');
            }

            if (!semaphoreV()) {
                LOG(LOG_ERROR, "can not release the semaphore, create SharedMemory failure");
            }
        }


        int32_t SharedMemoryManager::deleteSharedMemory() {

        }


        void SharedMemoryManager::getSharedMemoryID() {

        }

        /**
         *
         * @return the SharedMemoryBucket object
         */
        void SharedMemoryManager::openSMBucket() {
            LOG(INFO, "come in the openSMBucket");
            file_mapping m_file(SHARED_MEMORY_PATH_FILE_NAME, read_write);

            std::shared_ptr<mapped_region> tmp(new mapped_region(m_file, read_write));
            regionPtr = tmp;

//            printSMStatus();
        }


        void SharedMemoryManager::printSMStatus() {

            void *addr = regionPtr->get_address();
//            std::size_t size = regionPtr->get_size();

            const char *mem = static_cast<char *> (addr);

            int length = 0;
            char type = *(mem + length);
            LOG(INFO, "semaphore flag = %c", type);
            length = length + 1;
            while (length < SM_FILE_SIZE) {
                type = *(mem + length);
                LOG(INFO, "type = %c", type);
                length = length + 1 + 8 + FILENAME_MAX_LENGTH;
            }
        }


        void SharedMemoryManager::closeSMFile() {
            close(sharedMemoryID);
        }

        void SharedMemoryManager::closeSMBucket() {
            int res = shmdt(static_cast<void *>(sharedMemoryBucket.get()));
            if (res == -1) {
                cout << "some error occur, can not shmdt the shared memory" << endl;
            }
        }


        bool SharedMemoryManager::checkAndSetSMOne() {
            char *mem = static_cast<char *>(regionPtr->get_address());
//            char c = mem[0];
//            LOG(INFO, "checkAndSetSM1 before *mem = %d", *mem);
//            LOG(INFO, "checkAndSetSM1 before *mem  first = %d", ((c >> 0) & 1));

            if (*(mem) != '0') {
                LOG(LOG_ERROR, "shared memory is broken");
                return true;
            } else {
                *mem = '1';
            }


            /**
             * this is int
             *
            if (*((int *) (mem)) != 0) {
                LOG(LOG_ERROR, "shared memory is broken");
                return true;
            } else {
                int *tmp = (int *) (mem);
                *tmp = 1;
//                bitSet(mem, 1, 1);
            }
             **/
//            c = mem[0];
//            LOG(INFO, "checkAndSetSM1 after *mem = %d", *mem);
//            LOG(INFO, "checkAndSetSM1 after *mem  first = %d", ((c >> 0) & 1));
            return false;
        }

        void SharedMemoryManager::checkAndSetSMZero() {
            char *mem = static_cast<char *>(regionPtr->get_address());
//            char c = mem[0];
//            LOG(INFO, " checkAndSetSM0 before *mem = %d", *mem);
//            LOG(INFO, " checkAndSetSM0 before *mem  first = %d", ((c >> 0) & 1));

//            int *tmp = (int *) (mem);
//            *tmp = 0;

            *mem = '0';

////            bitSet(mem, 1, 0);
//            c = mem[0];
//            LOG(INFO, "checkAndSetSM0 after *mem = %d", *mem);
//            LOG(INFO, "checkAndSetSM0 after *mem  first = %d", ((c >> 0) & 1));
        }


        std::vector<int> SharedMemoryManager::acquireNewBlock(char *fileName) {
            LOG(INFO, "acquireNewBlock method");
            if (strlen(fileName) > FILENAME_MAX_LENGTH) {
                LOG(LOG_ERROR, "fileName name size cannot be larger than %d, ", FILENAME_MAX_LENGTH);
            }

            if (!semaphoreP()) {
                LOG(LOG_ERROR, "can not acquire the semaphore, acquireNewBlock failure");
            }
            if (checkAndSetSMOne()) {
                LOG(LOG_ERROR, "acquireNewBlock failed, shared memory is broken");
            }

            std::vector<int> resVector;

            int i = 1;
            int length = 1;
            char *mem = static_cast<char *>(regionPtr->get_address());

            // 1. acquire new block lists
            while (length < SM_FILE_SIZE) {
                char type = *(mem + length);
                if (type == '0') {
                    //1.write type
                    *(mem + length) = '1';

                    //2. write start time

                    //3. write fileName
                    length = length + 1 + 8;
                    memcpy(mem + length, fileName, strlen(fileName));
                    LOG(INFO, "file size = %d", strlen(fileName));


                    resVector.push_back(i - 1);
                    length = length + FILENAME_MAX_LENGTH;

                    if (resVector.size() >= QUOTA_SIZE) {
                        break;
                    }
                } else {
                    length = length + 1 + 8 + FILENAME_MAX_LENGTH;
                }

                i++;
            }

            checkAndSetSMZero();
            if (!semaphoreV()) {
                LOG(LOG_ERROR, "can not release the semaphore, acquireNewBlock failure");
            }

            if (length >= SM_FILE_SIZE) {
                LOG(LOG_ERROR, "no enough room for the ssd bucket");
            }

            printSMStatus();
            return resVector;

        }


        void SharedMemoryManager::inactiveBlock(int blockID) {
            if (!checkBlockIDIsLegal(blockID)) {
                return;
            }

            if (!semaphoreP()) {
                LOG(LOG_ERROR, "can not acquire the semaphore, acquireNewBlock failure");
            }
            if (checkAndSetSMOne()) {
                LOG(LOG_ERROR, "acquireNewBlock failed, shared memory is broken");
            }

            int length = 1;
            char *mem = static_cast<char *>(regionPtr->get_address());
            length = length + (1 + 8 + FILENAME_MAX_LENGTH) * blockID;

            *(mem + length) = '2';

            if (length >= SM_FILE_SIZE) {
                LOG(LOG_ERROR, "given block id = %d exceed the max size which is ");
            }

            checkAndSetSMZero();
            if (!semaphoreV()) {
                LOG(LOG_ERROR, "can not release the semaphore, acquireNewBlock failure");
            }

            printSMStatus();
        }


        void SharedMemoryManager::releaseBlock(int blockID) {

            if (!checkBlockIDIsLegal(blockID)) {
                return;
            }

            if (!semaphoreP()) {
                LOG(LOG_ERROR, "can not acquire the semaphore, acquireNewBlock failure");
            }
            if (checkAndSetSMOne()) {
                LOG(LOG_ERROR, "acquireNewBlock failed, shared memory is broken");
            }

            int length = 1;
            char *mem = static_cast<char *>(regionPtr->get_address());
            length = length + (1 + 8 + FILENAME_MAX_LENGTH) * blockID;

            *(mem + length) = '0';

            checkAndSetSMZero();
            if (!semaphoreV()) {
                LOG(LOG_ERROR, "can not release the semaphore, acquireNewBlock failure");
            }

            printSMStatus();
        }


        void SharedMemoryManager::evictBlock(int blockID) {
            if (!checkBlockIDIsLegal(blockID)) {
                return;
            }

            if (!semaphoreP()) {
                LOG(LOG_ERROR, "can not acquire the semaphore, acquireNewBlock failure");
            }
            if (checkAndSetSMOne()) {
                LOG(LOG_ERROR, "acquireNewBlock failed, shared memory is broken");
            }

            int length = 1;
            char *mem = static_cast<char *>(regionPtr->get_address());
            length = length + (1 + 8 + FILENAME_MAX_LENGTH) * blockID;

            *(mem + length) = '1';


            checkAndSetSMZero();
            if (!semaphoreV()) {
                LOG(LOG_ERROR, "can not release the semaphore, acquireNewBlock failure");
            }
            printSMStatus();
        }

        bool SharedMemoryManager::checkBlockIDIsLegal(int blockID) {
            int maxBlockID = (SM_FILE_SIZE - 1) / (1 + 8 + FILENAME_MAX_LENGTH);
            if (blockID >= maxBlockID) {
                LOG(LOG_ERROR, "given block id %d exceed the max size which is %d ", blockID, maxBlockID);
                return false;
            }
            return true;
        }





//        void SharedMemoryManager::bitSet(char *p_data, int32_t position, int flag) {
//            if (position > 8 || position < 1 || (flag != 0 && flag != 1)) {
//                LOG(LOG_ERROR, "in the bitSet method, the position is illegal");
//                return;
//            }
//            if (flag != (*p_data >> (position - 1) & 1)) {
//                *p_data ^= 1 << (position - 1);
//            }
//        }


//        char *SharedMemoryManager::generateStr(int length) {
//            char str[length + 1];
//            int i, flag;
//
//            srand(time(NULL));
//            for (i = 0; i < length; i++) {
//                flag = rand() % 3;
//                switch (flag) {
//                    case 0:
//                        str[i] = rand() % 26 + 'a';
//                        break;
//                    case 1:
//                        str[i] = rand() % 26 + 'A';
//                        break;
//                    case 2:
//                        str[i] = rand() % 10 + '0';
//                        break;
//                }
//            }
//            return str;
//        }



        /**



        int SharedMemoryManager::acquireNewBlock() {
            LOG(INFO, "acquireNewBlock method");
            if (!semaphoreP()) {
                LOG(LOG_ERROR, "can not acquire the semaphore, acquireNewBlock failure");
            }

            if (checkAndSetSMOne()) {
                LOG(LOG_ERROR, "acquireNewBlock failed, shared memory is broken");
                return -1;
            }


            int i = 1;
            int j = 0;
            char *mem = static_cast<char *>(regionPtr->get_address());

            bool flag = true;

            while (i < SM_FILE_SIZE && flag) {
                char c = mem[i];
                for (j = 0; j < 8; j++) {
//                    LOG(INFO, "the if condition= %d",((c >> j)^1));
                    if (((c >> j) ^ 1) == 1) {
                        bitSet(&c, j + 1, 1);
//                        LOG(INFO, "i = %d,j = %d",i,j);
                        flag = false;
                        break;
                    }
                }
                //1.1 set bit to the shared memory
                memset(mem + i, c, 1);
                //1.2 TODO, set bit to shared memory file, this is automatically done bu file mapping
                if (j >= 8) {
                    i++;
                }
            }

            checkAndSetSMZero();

            if (!semaphoreV()) {
                LOG(LOG_ERROR, "can not release the semaphore, acquireNewBlock failure");
            }

            if (i >= SM_FILE_SIZE) {
                LOG(INFO, "no enough room for the ssd bucket");
            }

            int res = i * 8 + j - BUCKET_ID_BASE_OFFSET;
//            LOG(INFO, "the really bucket num = %d",res);
            return i * 8 + j - BUCKET_ID_BASE_OFFSET;

        }

         **/


        // ******************* semaphore **************************

        int SharedMemoryManager::checkSemaphore() {
            key_t key = ftok(SHARED_MEMORY_PATH_FILE_NAME, 1);
            int tmpSemID = semget(key, 1, 0666 | IPC_CREAT | IPC_EXCL);

            //this means the semaphore already exist, get the semaphoreID
            if (tmpSemID == -1) {
                //get the semaphore
                LOG(INFO, "semaphore already exist, acquire the semaphore");
                createSemaphore();
            } else {
                //create the semaphore
                LOG(INFO, "semaphore do not exist, create a new one and init it");
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
            LOG(INFO, "setSemaphoreValue method");

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
            LOG(INFO, "semaphoreP method");

            checkSemaphore();

            //P（sv）
            struct sembuf sem_b;
            sem_b.sem_num = 0;
            sem_b.sem_op = -1;//P()
            sem_b.sem_flg = SEM_UNDO;
            if (semop(semaphoreID, &sem_b, 1) == -1) {
                LOG(LOG_ERROR, "semaphoreP failed");
                return 0;
            }
            return 1;
        }

        int SharedMemoryManager::semaphoreV() {
            LOG(INFO, "semaphoreV method");
            //V（sv）
            struct sembuf sem_b;
            sem_b.sem_num = 0;
            sem_b.sem_op = 1;//V()
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