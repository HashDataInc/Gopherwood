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

#ifndef _GOPHERWOOD_COMMON_CONFIGURATION_H_
#define _GOPHERWOOD_COMMON_CONFIGURATION_H_

#include <string>

namespace Gopherwood {
namespace Internal {

class Configuration {
public:
    static std::string LOCAL_SPACE_FILE;
    static std::string SHARED_MEMORY_NAME;
    static std::string MANIFEST_FOLDER;
    static int32_t NUMBER_OF_BLOCKS;
    static int64_t LOCAL_BUCKET_SIZE;
    static uint16_t MAX_CONNECTION;
    static int CUR_CONNECTION;
    static uint32_t PRE_ALLOCATE_BUCKET_NUM;
    static int32_t PRE_ACTIVATE_BLOCK_NUM;

    /* hard coded parameters */
    static size_t MAX_LOADER_THREADS;

    static uint32_t getCurQuotaSize();
};


}
}

#endif /* _GOPHERWOOD_COMMON_CONFIGURATION_H_ */
