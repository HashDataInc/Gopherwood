

#include "FSConfig.h"
#include "SharedMemoryManager.h"
namespace Gopherwood {

    namespace Internal {
        int64_t SHM_SIZE = 4096;//the size of shared memory
        int64_t SIZE_OF_FILE = 20 * 1024;//the size of the SSD file, default is 20GB
        int64_t SIZE_OF_BLOCK = 1 * 1024; //the size of the bucket size

        int32_t BIT_MAP_SIZE = 40;

        char *BUCKET_PATH_FILE_NAME = "/ssdfile/ssdkv/gopherwood";
//        char *sharedMemoryPath = "/ssdfile/ssdkv/sharedMemory/";
//
//        char *sharedMemoryFileName = "smFile";// the file which save the key of the shared memory


        char *SHARED_MEMORY_PATH_FILE_NAME = "/ssdfile/ssdkv/sharedMemory/smFile";

        char *FILE_LOG_PERSISTENCE_PATH = "/ssdfile/ssdkv/logPersistence/";
        int FILENAME_MAX_LENGTH = 255;
        int NUMBER_OF_BLOCKS = 20;//char+(char+long+int(size of file name)+char[255])
        int QUOTA_SIZE = 5;

        int QINGSTOR_BUFFER_SIZE = 4 * 1024 * 1024;
        int32_t READ_BUFFER_SIZE = SIZE_OF_BLOCK/4;
        int WRITE_BUFFER_SIZE = 8 * 1024 * 1024;
    }


}
