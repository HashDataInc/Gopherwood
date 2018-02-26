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
#include "SharedMemoryContext.h"

namespace Gopherwood {
namespace Internal {

SharedMemoryContext::SharedMemoryContext(std::string dir, shared_ptr<mapped_region> region,
        shared_ptr<named_semaphore> semaphore) :
        workDir(dir), mShareMem(region), mSemaphore(semaphore) {
}

void SharedMemoryContext::reset() {
    std::memset(mShareMem->get_address(), 0, mShareMem->get_size());
}

std::vector<int32_t> SharedMemoryContext::acquireBlock(FileId fileId)
{
    std::vector<int32_t> res;

    getMutex();

    

    releaseMutex();

    return res;
}

int SharedMemoryContext::calcBlockAcquireNum()
{
    return 1;
}

/* TODO: Use timed_wait() */
void SharedMemoryContext::getMutex()
{
    mSemaphore->wait();
}

void SharedMemoryContext::releaseMutex()
{
    mSemaphore->post();
}

}
}
