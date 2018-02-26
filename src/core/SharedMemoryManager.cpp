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
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Configuration.h"
#include "common/Logger.h"
#include "core/SharedMemoryManager.h"

using namespace boost::interprocess;

namespace Gopherwood {
namespace Internal {

/**
 * buildSharedMemoryContext - build shared memory context
 * This is a GopherWood instance for communication with other instances.
 * @param   workDir The working directory of current instance.
 */
shared_ptr<SharedMemoryContext> SharedMemoryManager::buildSharedMemoryContext(const char* workDir) {
    bool shmExist = true;
    shared_ptr<SharedMemoryContext> ctx;
    shared_ptr < shared_memory_object > shm;
    shared_ptr < named_semaphore > semaphore;

    /* open/create semaphore for this context */
    semaphore = openSemaphore("semaphore");

    /* try to open the shared memory */
    shm = openSharedMemory(Configuration::SHARED_MEMORY_NAME.c_str(), &shmExist);

    /* create the Shared Memory if not exists */
    if (!shmExist) {
        semaphore->wait();

        /* create Shared Memory */
        shm = createSharedMemory(Configuration::SHARED_MEMORY_NAME.c_str());
        /* set Shared Memory size */
        shm->truncate(1 + Configuration::NUMBER_OF_BLOCKS * sizeof(ShareMemBucket));

        semaphore->post();
    }

    /* map the Shared Memory for this context */
    shared_ptr < mapped_region > region(new mapped_region(*shm, read_write));

    ctx = shared_ptr < SharedMemoryContext > (new SharedMemoryContext(workDir, region, semaphore));

    /* TODO: Rebuild Shared Memory status from existing manifest logs */
    if (!shmExist) {
        rebuildShmFromManifest(ctx);
    }

    return ctx;
}

shared_ptr<shared_memory_object> SharedMemoryManager::createSharedMemory(const char* name) {
    shared_ptr < shared_memory_object > res;
    try {
        res = shared_ptr < shared_memory_object
                > (new shared_memory_object(create_only, name, read_write));
    } catch (const interprocess_exception & e) {
        LOG(
                WARNING,
                "Got exception when open/create the Shared Memory, error message: %s",
                e.what());
        THROW(
                GopherwoodSyncException,
                "[SharedMemoryManager::createSharedMemory] Got error when create Shared Memory, error code %d"
                        ", error message %s",
                e.get_error_code(),
                e.what());
    }
    return res;
}

shared_ptr<shared_memory_object> SharedMemoryManager::openSharedMemory(const char* name,
        bool* exist) {
    shared_ptr < shared_memory_object > res;
    try {
        res = shared_ptr < shared_memory_object
                > (new shared_memory_object(create_only, name, read_write));
    } catch (const interprocess_exception & e) {
        LOG(WARNING, "Got exception when opening the Shared Memory, error message: %s", e.what());
        *exist = false;
    }
    return res;
}

shared_ptr<named_semaphore> SharedMemoryManager::openSemaphore(const char* name) {
    shared_ptr < named_semaphore > res;
    try {
        res = shared_ptr < named_semaphore > (new named_semaphore(open_or_create, name, 1));
    } catch (const interprocess_exception & e) {
        LOG(
                WARNING,
                "Got exception when open/create the Shared Memory, error message: %s",
                e.what());
        THROW(
                GopherwoodSyncException,
                "[SharedMemoryManager::openSemaphore] Got error when open/create the semaphore, error code %d"
                        ", error message %s",
                e.get_error_code(),
                e.what());
    }
    return res;
}

void SharedMemoryManager::rebuildShmFromManifest(shared_ptr<SharedMemoryContext> ctx) {
    ctx->reset();
}

}
}
