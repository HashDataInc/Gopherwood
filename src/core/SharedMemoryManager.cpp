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
shared_ptr<SharedMemoryContext> SharedMemoryManager::buildSharedMemoryContext(const char *workDir, int32_t lockFD) {
    LOG(INFO, "Start SharedMemoryManager::buildSharedMemoryContext");
    bool shmExist = true;
    shared_ptr<SharedMemoryContext> ctx;
    shared_ptr<shared_memory_object> shm;
    shared_ptr<mapped_region> region;

    /* TODO: remove this line !*/
    shared_memory_object::remove(Configuration::SHARED_MEMORY_NAME.c_str());
    /* try to open the shared memory */
    shm = openSharedMemory(Configuration::SHARED_MEMORY_NAME.c_str(), &shmExist);

    /* create the Shared Memory if not exists */
    if (!shmExist) {
        /* get mutex */
        lockf(lockFD, F_LOCK, 0);

        /* create Shared Memory */
        shm = createSharedMemory(Configuration::SHARED_MEMORY_NAME.c_str());
        int size = sizeof(ShareMemHeader) + Configuration::NUMBER_OF_BLOCKS * sizeof(ShareMemBucket);
        shm->truncate(size);
        offset_t size1;
        shm->get_size(size1);
        region = shared_ptr<mapped_region>(new mapped_region(*shm, read_write));

        /* Init Shared Memory */
        void *addr = region->get_address();
        ShareMemHeader *header = static_cast<ShareMemHeader *>(addr);
        header->enter();
        header->reset(Configuration::NUMBER_OF_BLOCKS);
        LOG(INFO, "[SharedMemoryManager::buildSharedMemoryContext] num free bucket %d, "
                "num active buckets %d, num used buckets %d ",
                header->numFreeBuckets, header->numActiveBuckets, header->numUsedBuckets);
        memset((char *) addr + sizeof(ShareMemHeader), 0, Configuration::NUMBER_OF_BLOCKS * sizeof(ShareMemBucket));
        header->exit();

        /* release mutex */
        lockf(lockFD, F_ULOCK, 0);
    } else {
        try {
            region = shared_ptr<mapped_region>(new mapped_region(*shm, read_write));
        } catch (const interprocess_exception &e) {
            LOG(
                    WARNING,
                    "Got exception when mapping region, error message: %s",
                    e.what());
            THROW(
                    GopherwoodSyncException,
                    "[SharedMemoryManager::createSharedMemory] Got exception when mapping region, error code %d"
                            ", error message %s",
                    e.get_error_code(),
                    e.what());
        }
    }

    ctx = shared_ptr<SharedMemoryContext>(new SharedMemoryContext(workDir, region, lockFD));

    /* TODO: Rebuild Shared Memory status from existing manifest logs */
    if (!shmExist) {
        rebuildShmFromManifest(ctx);
    }
    LOG(INFO, "End SharedMemoryManager::buildSharedMemoryContext");
    return ctx;
}

shared_ptr<shared_memory_object> SharedMemoryManager::createSharedMemory(const char *name) {
    shared_ptr<shared_memory_object> res;
    try {
        res = shared_ptr<shared_memory_object
        >(new shared_memory_object(create_only, name, read_write));
    } catch (const interprocess_exception &e) {
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

shared_ptr<shared_memory_object> SharedMemoryManager::openSharedMemory(const char *name,
                                                                       bool *exist) {
    shared_ptr<shared_memory_object> res;
    try {
        res = shared_ptr<shared_memory_object>(new shared_memory_object(open_only, name, read_write));
    } catch (const interprocess_exception &e) {
        LOG(WARNING, "Got exception when opening the Shared Memory, error message: %s", e.what());
        *exist = false;
    }
    return res;
}

void SharedMemoryManager::rebuildShmFromManifest(shared_ptr<SharedMemoryContext> ctx) {
}

shared_ptr<SharedMemoryManager> SharedMemoryManager::instance = NULL;

}
}
