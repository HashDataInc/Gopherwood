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

            //IMPORTANT,init the shared memory to '0', not 0
            for (int i = 0; i < TOTAL_SHARED_MEMORY_LENGTH; i++) {
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
//            LOG(INFO, "come in the openSMBucket");
            file_mapping m_file(SHARED_MEMORY_PATH_FILE_NAME, read_write);

            std::shared_ptr<mapped_region> tmp(new mapped_region(m_file, read_write));
            regionPtr = tmp;

//            printSMStatus();
        }


        void SharedMemoryManager::printSMStatus() {
            void *mem = regionPtr->get_address();
            int length = 0;
            char sempType = *((char *) (mem + length));
            LOG(INFO, "semaphore flag = %c", sempType);
            length = length + 1;
            smBucketStruct *smb;
            LOG(INFO, "TOTAL_SHARED_MEMORY_LENGTH  = %d", TOTAL_SHARED_MEMORY_LENGTH);

            while ((length < TOTAL_SHARED_MEMORY_LENGTH)) {
                smb = (smBucketStruct *) (mem + length);
                char type = smb->type;
                int blockIndex = smb->blockIndex;
                char fileName[256];
                memcpy(fileName, smb->fileName, sizeof(fileName));
//                LOG(INFO, "type = %c, blockIndex = %d, fileName=%s", type, blockIndex, fileName);
                LOG(INFO, "type = %c, length = %d", type, length);
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

            if (!semaphoreP()) {
                LOG(LOG_ERROR, "can not acquire the semaphore, acquireNewBlock failure");
            }
            if (checkAndSetSMOne()) {
                LOG(LOG_ERROR, "acquireNewBlock failed, shared memory is broken");
            }


            void *mem = regionPtr->get_address();
            smBucketStruct *smb;
            int length = 1 + sizeof(smBucketStruct) * blockID;
            smb = (smBucketStruct *) (mem + length);
            int index = smb->blockIndex;

            checkAndSetSMZero();
            if (!semaphoreV()) {
                LOG(LOG_ERROR, "can not release the semaphore, acquireNewBlock failure");
            }
            return index;
        }


        std::vector<int> SharedMemoryManager::acquireNewBlock(char *fileName) {
//            LOG(INFO, "acquireNewBlock method");
            if (strlen(fileName) > FILENAME_MAX_LENGTH) {
                LOG(LOG_ERROR, "SharedMemoryManager::acquireNewBlock. fileName name size cannot be larger than %d, ", FILENAME_MAX_LENGTH);
            }

            if (!semaphoreP()) {
                LOG(LOG_ERROR, "SharedMemoryManager::acquireNewBlock. can not acquire the semaphore, acquireNewBlock failure");
            }
            if (checkAndSetSMOne()) {
                LOG(LOG_ERROR, "SharedMemoryManager::acquireNewBlock. acquireNewBlock failed, shared memory is broken");
            }

            std::vector<int> resVector;

            int i = 0;
            int length = 1;
            void *mem = regionPtr->get_address();
            smBucketStruct *smb;


            // 1. acquire new block lists
            while (length < TOTAL_SHARED_MEMORY_LENGTH) {
                smb = (smBucketStruct *) (mem + length);

                char type = smb->type;
                if (type == '0') {
                    //1.write type
                    smb->type = '1';

                    //3. write size of file name
                    string strFileName;
                    strFileName.append(fileName, strlen(fileName));
                    strFileName.append(1, '\0');
                    memcpy(smb->fileName, strFileName.data(), strFileName.size());

//                    LOG(INFO, "file size = %d", strlen(fileName));

                    resVector.push_back(i);

                    if (resVector.size() >= QUOTA_SIZE) {
                        break;
                    }
                }

                length = length + sizeof(smBucketStruct);
                i++;
            }

            checkAndSetSMZero();
            if (!semaphoreV()) {
                LOG(LOG_ERROR, "SharedMemoryManager::acquireNewBlock. can not release the semaphore, acquireNewBlock failure");
            }

            if (length >= TOTAL_SHARED_MEMORY_LENGTH) {
                LOG(LOG_ERROR, "SharedMemoryManager::acquireNewBlock. no enough room for the ssd bucket");
            }

            printSMStatus();
            return resVector;
        }


        std::vector<int> SharedMemoryManager::getBlocksWhichTypeEqual2(int count) {

            if (!semaphoreP()) {
                LOG(LOG_ERROR, "SharedMemoryManager::getBlocksWhichTypeEqual2. can not acquire the semaphore, acquireNewBlock failure");
            }
            if (checkAndSetSMOne()) {
                LOG(LOG_ERROR, "SharedMemoryManager::getBlocksWhichTypeEqual2. acquireNewBlock failed, shared memory is broken");
            }

            std::vector<int> resVector;

            int i = 0;
            int length = 1;
            void *mem = regionPtr->get_address();
            smBucketStruct *smb;

            // 1. get the  new block lists which type = '2'
            while (length < TOTAL_SHARED_MEMORY_LENGTH) {
                smb = (smBucketStruct *) (mem + length);
                char type = smb->type;
                if (type == '2') {
                    resVector.push_back(i);

                    if (resVector.size() >= count) {
                        break;
                    }
                }
                length = length + sizeof(smBucketStruct);
                i++;
            }
            checkAndSetSMZero();
            if (!semaphoreV()) {
                LOG(LOG_ERROR, "SharedMemoryManager::getBlocksWhichTypeEqual2. can not release the semaphore, acquireNewBlock failure");
            }
            if (length >= TOTAL_SHARED_MEMORY_LENGTH) {
                LOG(LOG_ERROR, "SharedMemoryManager::getBlocksWhichTypeEqual2. no enough room for the ssd bucket");
            }
            return resVector;
        }


        void SharedMemoryManager::inactiveBlock(int blockID, int blockIndex) {
            if (!checkBlockIDIsLegal(blockID)) {
                return;
            }

            if (!semaphoreP()) {
                LOG(LOG_ERROR, "SharedMemoryManager::inactiveBlock. can not acquire the semaphore, acquireNewBlock failure");
            }
            if (checkAndSetSMOne()) {
                LOG(LOG_ERROR, "SharedMemoryManager::inactiveBlock. acquireNewBlock failed, shared memory is broken");
            }

            int length = 1;
            void *mem = regionPtr->get_address();
            length += sizeof(smBucketStruct) * blockID;
            smBucketStruct *smb = (smBucketStruct *) (mem + length);

            //1.change the type
            smb->type = '2';

            //2. set the block index
            smb->blockIndex = blockIndex;


            if (length >= TOTAL_SHARED_MEMORY_LENGTH) {
                LOG(LOG_ERROR, "SharedMemoryManager::inactiveBlock. given block id = %d exceed the max size which is ");
            }

            checkAndSetSMZero();
            if (!semaphoreV()) {
                LOG(LOG_ERROR, "SharedMemoryManager::inactiveBlock. can not release the semaphore, acquireNewBlock failure");
            }

            printSMStatus();
        }


        void SharedMemoryManager::releaseBlock(int blockID) {

            if (!checkBlockIDIsLegal(blockID)) {
                return;
            }

            if (!semaphoreP()) {
                LOG(LOG_ERROR, "SharedMemoryManager::releaseBlock. can not acquire the semaphore, acquireNewBlock failure");
            }
            if (checkAndSetSMOne()) {
                LOG(LOG_ERROR, "SharedMemoryManager::releaseBlock. acquireNewBlock failed, shared memory is broken");
            }

            int length = 1;
            void *mem = regionPtr->get_address();
            length += sizeof(smBucketStruct) * blockID;
            smBucketStruct *smb = (smBucketStruct *) (mem + length);

            smb->type = '0';

            checkAndSetSMZero();
            if (!semaphoreV()) {
                LOG(LOG_ERROR, "SharedMemoryManager::releaseBlock. can not release the semaphore, acquireNewBlock failure");
            }

            printSMStatus();
        }

        std::string SharedMemoryManager::getFileNameAccordingBlockID(int blockID) {
            if (!checkBlockIDIsLegal(blockID)) {
                return NULL;
            }

            if (!semaphoreP()) {
                LOG(LOG_ERROR, "SharedMemoryManager::releaseBlock. can not acquire the semaphore, acquireNewBlock failure");
            }
            if (checkAndSetSMOne()) {
                LOG(LOG_ERROR, "SharedMemoryManager::releaseBlock. acquireNewBlock failed, shared memory is broken");
            }

            int length = 1;
            void *mem = regionPtr->get_address();
            length += sizeof(smBucketStruct) * blockID;

            smBucketStruct *smb = (smBucketStruct *) (mem + length);


            char tmpName[256];

            memcpy(tmpName, smb->fileName, sizeof(smb->fileName));
//            LOG(INFO, " previous file name = %s", tmpName);

            string retVal;
            retVal.append(tmpName, sizeof(smb->fileName));
            checkAndSetSMZero();
            if (!semaphoreV()) {
                LOG(LOG_ERROR, "SharedMemoryManager::releaseBlock. can not release the semaphore, acquireNewBlock failure");
            }
            return retVal;
        }

        void SharedMemoryManager::evictBlock(int blockID, char *fileName) {
            if (!checkBlockIDIsLegal(blockID)) {
                return;
            }

            if (!semaphoreP()) {
                LOG(LOG_ERROR, "SharedMemoryManager::evictBlock. can not acquire the semaphore, acquireNewBlock failure");
            }
            if (checkAndSetSMOne()) {
                LOG(LOG_ERROR, "SharedMemoryManager::evictBlock. acquireNewBlock failed, shared memory is broken");
            }

            int length = 1;
            void *mem = regionPtr->get_address();
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

            checkAndSetSMZero();
            if (!semaphoreV()) {
                LOG(LOG_ERROR, "SharedMemoryManager::evictBlock. can not release the semaphore, acquireNewBlock failure");
            }
            printSMStatus();
        }

        bool SharedMemoryManager::checkBlockIDIsLegal(int blockID) {
            if (blockID > NUMBER_OF_BLOCKS - 1) {
                LOG(LOG_ERROR, "SharedMemoryManager::evictBlock. given block id %d exceed the max size which is %d ", blockID, NUMBER_OF_BLOCKS - 1);
                return false;
            }
            return true;
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
            sem_b.sem_op = -1;//P()
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