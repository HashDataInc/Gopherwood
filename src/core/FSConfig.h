/*
 * fsConfig.h
 *
 *  Created on: Nov 15, 2017
 *      Author: root
 */

#ifndef _GOPHERWOOD_CORE_FSCONFIG_H_
#define _GOPHERWOOD_CORE_FSCONFIG_H_

#include <cstdint>

namespace Gopherwood {
    namespace Internal {


        extern int32_t SHM_SIZE;//the size of shared memory
        extern int32_t SIZE_OF_FILE;//= 20 * 1024 * 1024 * 1024;//the size of the SSD file, default is 20GB
        extern int32_t SIZE_OF_BLOCK;// = 64 * 1024 * 1024; //the size of the bucket size
//const int32_t BIT_MAP_SIZE=SIZE_OF_FILE/(SIZE_OF_BLOCK*8);
        extern int32_t BIT_MAP_SIZE;// = 40;

        extern char *bucketFilePath;// = "/ssdfile/ssdkv/gopherwood";
        extern char *sharedMemoryFileName;// = "smFile";// the file which save the key of the shared memory

        extern char *sharedMemoryPath;// = "/ssdfile/ssdkv/sharedMemory/";


        extern char *smPathFileName;// = "/ssdfile/ssdkv/sharedMemory/smFile";
        extern int SM_FILE_SIZE;
//        extern char *SHARED_MEMORY_KEY;
    }


}


#endif /* _GOPHERWOOD_CORE_FSCONFIG_H_ */
