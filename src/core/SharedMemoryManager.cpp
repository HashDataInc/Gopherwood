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
#include "common/Configuration.h"
#include "common/Logger.h"
#include "core/SharedMemoryManager.h"

namespace Gopherwood {
namespace Internal {

shared_ptr<SharedMemoryContext> SharedMemoryManager::buildSharedMemoryContext(const char* workDir) {
    bool createNewShm = false;
    shared_ptr<shared_memory_object> shm;
    shared_ptr<SharedMemoryContext> ctx;

    /* try to open the shared memory */
    try
    {
        shm = shared_ptr<shared_memory_object>(
                new shared_memory_object(open_only, Configuration::SHARED_MEMORY_NAME.c_str(), read_only));
    }
    catch (const interprocess_exception & e){
        LOG(WARNING, "Got exception when opening the Shared Memory, error message: %s", e.what());
        createNewShm = true;
    }

    /* create the Shared Memory if not exists */
    if (createNewShm)
    {
        /* create Shared Memory */
        shm = createSharedMemory(Configuration::SHARED_MEMORY_NAME.c_str());
        /* set Shared Memory size */
        shm->truncate(1 + Configuration::NUMBER_OF_BLOCKS * sizeof(shmBucket));
    }

    /* map the Shared Memory to this process */
    shared_ptr<mapped_region> region(new mapped_region(*shm, read_write));

    ctx = shared_ptr <SharedMemoryContext> (new SharedMemoryContext(workDir, region));

    /* TODO: Rebuild Shared Memory status from existing manifest logs */
    if (createNewShm)
    {
        rebuildShmFromManifest(ctx);
    }

    return ctx;
}

shared_ptr<shared_memory_object> SharedMemoryManager::createSharedMemory(const char* name)
{
    return shared_ptr<shared_memory_object>(new shared_memory_object(create_only, name, read_write));
}

void SharedMemoryManager::rebuildShmFromManifest(shared_ptr<SharedMemoryContext> ctx)
{
    ctx->reset();
}

}
}
