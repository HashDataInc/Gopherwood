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
#include "core/ActiveStatusContext.h"

namespace Gopherwood {
namespace Internal {

ActiveStatusContext::ActiveStatusContext(shared_ptr<SharedMemoryContext> sharedMemoryContext) :
        mSharedMemoryContext(sharedMemoryContext) {
    registInSharedMem();
}

void ActiveStatusContext::registInSharedMem(){
    mSharedMemoryContext->lock();
    mConnId = mSharedMemoryContext->regist(getpid());
    if (mConnId == -1){
        THROW(GopherwoodSharedMemException,
              "[ActiveStatusContext::registInSharedMem] Exceed max connection limitation %d",
              mSharedMemoryContext->getNumMaxConn());
    }
    LOG(INFO, "[ActiveStatusContext::registInSharedMem] Registered successfully, ConnID=%d, PID=%d", mConnId, getpid());
    mSharedMemoryContext->unlock();
}

void ActiveStatusContext::unregistInSharedMem() {
    mSharedMemoryContext->lock();
    int rc = mSharedMemoryContext->unregist(mConnId, getpid());
    if (rc != 0){
        mSharedMemoryContext->unlock();
        THROW(GopherwoodSharedMemException,
              "[ActiveStatusContext::registInSharedMem] connection info mismatch with SharedMem ConnId=%d, PID=%d",
              mConnId, getpid());
    }
    LOG(INFO, "[ActiveStatusContext::registInSharedMem] Unregistered successfully, ConnID=%d, PID=%d", mConnId,getpid());
    mConnId = -1;
    mSharedMemoryContext->unlock();
}


shared_ptr<ActiveStatus> ActiveStatusContext::getFileActiveStatus(FileId fileId) {
    unordered_map<std::string, shared_ptr<ActiveStatus>>::iterator item =
            mActiveStatusMap.find(fileId.toString());
    if (item != mActiveStatusMap.end()) {
        return item->second;
    }
    return NULL;
}

shared_ptr<ActiveStatus> ActiveStatusContext::initFileActiveStatus(FileId fileId) {
    shared_ptr<ActiveStatus> activeStatus = shared_ptr<ActiveStatus>(new ActiveStatus(fileId, mSharedMemoryContext));
    mActiveStatusMap.insert(make_pair(fileId.toString(), activeStatus));
    return activeStatus;
}

ActiveStatusContext::~ActiveStatusContext() {
    unregistInSharedMem();
}

}
}