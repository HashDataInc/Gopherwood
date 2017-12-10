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
        extern int32_t SIZE_OF_FILE;//the size of the SSD file, default is 20GB
        extern int32_t SIZE_OF_BLOCK;
        extern int32_t BIT_MAP_SIZE;

        extern char *BUCKET_PATH_FILE_NAME;

        extern char *SHARED_MEMORY_PATH_FILE_NAME;
        extern int SM_FILE_SIZE;
        extern char * FILE_LOG_PERSISTENCE_PATH;
        extern int BUCKET_ID_BASE_OFFSET;
    }


}


#endif /* _GOPHERWOOD_CORE_FSCONFIG_H_ */
