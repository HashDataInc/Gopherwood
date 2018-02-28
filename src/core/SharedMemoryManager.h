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
#ifndef _GOPHERWOOD_CORE_SHAREDMEMORYMANAGER_H_
#define _GOPHERWOOD_CORE_SHAREDMEMORYMANAGER_H_

#include "platform.h"

#include "common/Memory.h"
#include "core/SharedMemoryContext.h"

#include <boost/interprocess/creation_tags.hpp>
#include <boost/interprocess/exceptions.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/sync/named_semaphore.hpp>

namespace Gopherwood {
namespace Internal {

class SharedMemoryManager {
public:
    static shared_ptr<SharedMemoryManager> getInstance() {
        if (!instance)
            instance = shared_ptr < SharedMemoryManager > (new SharedMemoryManager());
        return instance;
    };

    shared_ptr<SharedMemoryContext> buildSharedMemoryContext(const char* workDir, int32_t lockFD);

private:
    shared_ptr<shared_memory_object> createSharedMemory(const char* name);

    shared_ptr<shared_memory_object> openSharedMemory(const char* name, bool* exist);

    void rebuildShmFromManifest(shared_ptr<SharedMemoryContext> ctx);

    static shared_ptr<SharedMemoryManager> instance;
};

}
}

#endif //_GOPHERWOOD_CORE_SHAREDMEMORYMANAGER_H_
