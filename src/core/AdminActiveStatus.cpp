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

#include "core/AdminActiveStatus.h"
#include "common/Configuration.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Logger.h"

namespace Gopherwood {
namespace Internal {


#define SHARED_MEM_BEGIN    try { \
                                mSharedMemoryContext->lock();

#define SHARED_MEM_END          mSharedMemoryContext->unlock();\
                            } catch (...) { \
                                SetLastException(Gopherwood::current_exception()); \
                                mSharedMemoryContext->unlock(); \
                                Gopherwood::rethrow_exception(Gopherwood::current_exception()); \
                            }

AdminActiveStatus::AdminActiveStatus(shared_ptr<SharedMemoryContext> sharedMemoryContext,
                                     int localSpaceFD) :
        BaseActiveStatus(sharedMemoryContext, localSpaceFD) {
    SHARED_MEM_BEGIN
        registInSharedMem();
    SHARED_MEM_END
}

void AdminActiveStatus::registInSharedMem() {
    mActiveId = mSharedMemoryContext->registAdmin(getpid());
    if (mActiveId == -1) {
        THROW(GopherwoodSharedMemException,
              "[ActiveStatus::registInSharedMem] Exceed max connection limitation %d",
              mSharedMemoryContext->getNumMaxActiveStatus());
    }
    LOG(DEBUG1, "[ActiveStatus]          |"
            "Registered successfully, ActiveID=%d, PID=%d", mActiveId, getpid());
}
void AdminActiveStatus::unregistInSharedMem() {
    if (mActiveId == -1)
        return;

    int rc = mSharedMemoryContext->unregistAdmin(mActiveId, getpid());
    if (rc != 0) {
        mSharedMemoryContext->unlock();
        THROW(GopherwoodSharedMemException,
              "[ActiveStatus::unregistInSharedMem] connection info mismatch with SharedMem ActiveId=%d, PID=%d",
              mActiveId, getpid());
    }

    LOG(DEBUG1, "[ActiveStatus]          |"
            "Unregistered successfully, ActiveID=%d, PID=%d", mActiveId, getpid());
    mActiveId = -1;
}

AdminActiveStatus::~AdminActiveStatus() {
    unregistInSharedMem();
}

}
}