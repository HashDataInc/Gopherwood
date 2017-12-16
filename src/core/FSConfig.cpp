

#include "FSConfig.h"

namespace Gopherwood {

    namespace Internal {
        int64_t SHM_SIZE = 4096;//the size of shared memory
        int64_t SIZE_OF_FILE = 20 << 30;//the size of the SSD file, default is 20GB
        int64_t SIZE_OF_BLOCK = 8 << 20; //the size of the bucket size

        int32_t BIT_MAP_SIZE = 40;

        char *BUCKET_PATH_FILE_NAME = "/ssdfile/ssdkv/gopherwood";
//        char *sharedMemoryPath = "/ssdfile/ssdkv/sharedMemory/";
//
//        char *sharedMemoryFileName = "smFile";// the file which save the key of the shared memory


        char *SHARED_MEMORY_PATH_FILE_NAME = "/ssdfile/ssdkv/sharedMemory/smFile";

        char *FILE_LOG_PERSISTENCE_PATH = "/ssdfile/ssdkv/logPersistence/";
        int FILENAME_MAX_LENGTH = 255;
        int SM_FILE_SIZE = 1 + (1 + 8 + 255) * 20;//char+(char+long+char[255])
        int QUOTA_SIZE = 5;

        int QINGSTOR_BUFFER_SIZE = 4 << 20;
        int32_t READ_BUFFER_SIZE = 1 << 20;
        int WRITE_BUFFER_SIZE = 8 << 20;
    }


}
