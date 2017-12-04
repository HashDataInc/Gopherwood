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
            int sharedMemoryFd = open(smPathFileName, flags, 0777);
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
            file_mapping::remove(smPathFileName);
            std::filebuf fbuf;
            fbuf.open(smPathFileName, std::ios_base::in | std::ios_base::out
                                      | std::ios_base::trunc | std::ios_base::binary);
            fbuf.pubseekoff(SM_FILE_SIZE - 1, std::ios_base::beg);
//            fbuf.sputc(0);
            for(int i=0;i<SM_FILE_SIZE;i++){
                fbuf.sputc(0);
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
            file_mapping m_file(smPathFileName, read_write);

//            mapped_region region(m_file, read_write);
            std::shared_ptr<mapped_region> tmp(new mapped_region(m_file, read_write));
            regionPtr = tmp;


            //Get the address of the mapped region
            void *addr = regionPtr->get_address();
            std::size_t size = regionPtr->get_size();


            const char *mem = static_cast<char *> (addr);

//            LOG(INFO, "before size mem is= %d", strlen(mem));
//
//            for (int i = 0; i < SM_FILE_SIZE; i++) {
//                char c = mem[i];
//                for (int j = 7; j >= 0; j--) {
//                    LOG(INFO, "*mem++ = %d", ((c >> j) & 1));
//                }
//            }
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

        void SharedMemoryManager::bitSet(char *p_data, char position, int flag) {
            if (position > 8 || position < 1 || (flag != 0 && flag != 1)) {
                LOG(LOG_ERROR, "in the bitSet method, the position is illegal");
                return;
            }
            if (flag != (*p_data >> (position - 1) & 1)) {
                *p_data ^= 1 << (position - 1);
            }
        }


        int SharedMemoryManager::acquireNewBlock() {
            int i = 0;
            int j = 0;
            char *mem = static_cast<char *>(regionPtr->get_address());

            bool flag = true;

            while(i<SM_FILE_SIZE&&flag){
                char c = mem[i];
                for (j = 0; j < 8; j++) {
//                    LOG(INFO, "the if condition= %d",((c >> j)^1));
                    if (((c >> j)^1)==1) {
                        bitSet(&c, j + 1, 1);
//                        LOG(INFO, "i = %d,j = %d",i,j);
                        flag = false;
                        break;
                    }
                }
                //1.1 set bit to the shared memory
                memset(mem + i, c, 1);
                //1.2 TODO, set bit to shared memory file, this is automatically done bu file mapping
                if(j>=8){
                    i++;
                }
            }
            if (i >=SM_FILE_SIZE ) {
                LOG(LOG_ERROR, "no enough room for the ssd bucket");
            }
            return i * 8 + j;


        }
    }
}