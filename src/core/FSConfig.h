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

extern int64_t SHM_SIZE; //the size of shared memory
extern int64_t SIZE_OF_FILE; //the size of the SSD file, default is 20GB
extern int64_t SIZE_OF_BLOCK;
extern int32_t BIT_MAP_SIZE;

extern const char *BUCKET_PATH_FILE_NAME;

extern const char *SHARED_MEMORY_PATH_FILE_NAME;
extern int NUMBER_OF_BLOCKS;
extern const char *FILE_LOG_PERSISTENCE_PATH;
extern unsigned int FILENAME_MAX_LENGTH;
extern unsigned int MIN_QUOTA_SIZE;

extern int QINGSTOR_BUFFER_SIZE;
extern int32_t READ_BUFFER_SIZE;
extern int WRITE_BUFFER_SIZE;

extern int MAX_PROCESS; //maximum number of processes running at the same time.
extern int MAX_QUOTA_SIZE;

}

}

#endif /* _GOPHERWOOD_CORE_FSCONFIG_H_ */
