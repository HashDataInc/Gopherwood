

#include "FSConfig.h"

namespace Gopherwood {

    namespace Internal {
        int32_t SHM_SIZE = 4096;//the size of shared memory
        int32_t SIZE_OF_FILE = 20 * 1024 * 1024 * 1024;//the size of the SSD file, default is 20GB
        int32_t SIZE_OF_BLOCK = 64 * 1024 * 1024; //the size of the bucket size

        int32_t BIT_MAP_SIZE = 40;

        char *BUCKET_PATH_FILE_NAME = "/ssdfile/ssdkv/gopherwood";
//        char *sharedMemoryPath = "/ssdfile/ssdkv/sharedMemory/";
//
//        char *sharedMemoryFileName = "smFile";// the file which save the key of the shared memory


        char *SHARED_MEMORY_PATH_FILE_NAME = "/ssdfile/ssdkv/sharedMemory/smFile";

        char *FILE_LOG_PERSISTENCE_PATH = "/ssdfile/ssdkv/logPersistence/";
        int SM_FILE_SIZE = 2;
        int BUCKET_ID_BASE_OFFSET = 8;
    }


}
