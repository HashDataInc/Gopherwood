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
}

shared_ptr<ActiveStatus> ActiveStatusContext::initFileActiveStatus(FileId fileId, bool isWrite) {
    ActiveStatusType type = isWrite ? ActiveStatusType::writeFile : ActiveStatusType::readFile;

    shared_ptr<ActiveStatus> activeStatus =
            shared_ptr<ActiveStatus>(new ActiveStatus(fileId,
                                                      mSharedMemoryContext,
                                                      true, /* isCreate*/
                                                      type));
    return activeStatus;
}

shared_ptr<ActiveStatus> ActiveStatusContext::openFileActiveStatus(FileId fileId, bool isWrite) {
    ActiveStatusType type = isWrite ? ActiveStatusType::writeFile : ActiveStatusType::readFile;

    shared_ptr<ActiveStatus> activeStatus =
            shared_ptr<ActiveStatus>(new ActiveStatus(fileId,
                                                      mSharedMemoryContext,
                                                      false, /* isCreate*/
                                                      type));
    return activeStatus;
}

shared_ptr<ActiveStatus> ActiveStatusContext::deleteFileActiveStatus(FileId fileId){
    shared_ptr<ActiveStatus> activeStatus =
            shared_ptr<ActiveStatus>(new ActiveStatus(fileId,
                                                      mSharedMemoryContext,
                                                      false, /* isCreate*/
                                                      ActiveStatusType::deleteFile));
    return activeStatus;
}

ActiveStatusContext::~ActiveStatusContext() {
}

}
}