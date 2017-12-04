#include "FSConfig.h"

int SHM_SIZE = 4096;//the size of shared memory
int SIZE_OF_FILE = 20 * 1024 * 1024 * 1024;//the size of the SSD file, default is 20GB
int SIZE_OF_BLOCK = 64 * 1024 * 1024; //the size of the bucket size

int BIT_MAP_SIZE = 40;

char *bucketFilePath = "/ssdfile/ssdkv/gopherwood";
char *sharedMemoryPath = "/ssdfile/ssdkv/sharedMemory/";

char *sharedMemoryFileName = "smFile";// the file which save the key of the shared memory


char *smPathFileName = "/ssdfile/ssdkv/sharedMemory/smFile";

