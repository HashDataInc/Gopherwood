

#include "FSConfig.h"

namespace Gopherwood {

    namespace Internal {
        int32_t SHM_SIZE = 4096;//the size of shared memory
        int32_t SIZE_OF_FILE = 20 * 1024 * 1024 * 1024;//the size of the SSD file, default is 20GB
        int32_t SIZE_OF_BLOCK = 64 * 1024 * 1024; //the size of the bucket size

        int32_t BIT_MAP_SIZE = 40;

        char *bucketFilePath = "/ssdfile/ssdkv/gopherwood";
        char *sharedMemoryPath = "/ssdfile/ssdkv/sharedMemory/";

        char *sharedMemoryFileName = "smFile";// the file which save the key of the shared memory


        char *smPathFileName = "/ssdfile/ssdkv/sharedMemory/smFile";

        int SM_FILE_SIZE = 40;

//        char *SHARED_MEMORY_KEY = "gopherwood_shared_memory_key";
    }


}
