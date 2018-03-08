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


        extern int64_t SIZE_OF_BLOCK;
        extern char *BUCKET_PATH_FILE_NAME;
        extern char *SHARED_MEMORY_PATH_FILE_NAME;
        extern int NUMBER_OF_BLOCKS;
        extern char *FILE_LOG_PERSISTENCE_PATH;
        extern int FILENAME_MAX_LENGTH;
        extern int MIN_QUOTA_SIZE;

        extern int QINGSTOR_BUFFER_SIZE;
        extern int32_t READ_BUFFER_SIZE;
        extern int WRITE_BUFFER_SIZE;

        extern int MAX_PROCESS;//maximum number of processes running at the same time.
        extern int MAX_QUOTA_SIZE;

        extern char *LOG_FILE_PATH;

    }


}


#endif /* _GOPHERWOOD_CORE_FSCONFIG_H_ */
