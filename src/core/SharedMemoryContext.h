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
#ifndef _GOPHERWOOD_CORE_SHAREDMEMORYCONTEXT_H_
#define _GOPHERWOOD_CORE_SHAREDMEMORYCONTEXT_H_

#include "platform.h"
#include "common/Memory.h"

#include <boost/interprocess/mapped_region.hpp>

using namespace boost::interprocess;

namespace Gopherwood {
namespace Internal {

typedef struct shmBucket {
    char type;
    char isKick;
    int32_t blockIndex;
    char fileName[256];
} shmBucket;

class SharedMemoryContext {
public:
    SharedMemoryContext(std::string dir, shared_ptr<mapped_region> region,
            shared_ptr<named_semaphore> semaphore);

    int32_t acquireBlock();

    void reset();

    ~SharedMemoryContext();
private:
    std::string workDir;
    shared_ptr<mapped_region> mapped_shm;
    shared_ptr<named_semaphore> semaphore;
};

}
}

#endif //_GOPHERWOOD_CORE_SHAREDMEMORYCONTEXT_H_
