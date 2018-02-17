/********************************************************************
 * 2017 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
