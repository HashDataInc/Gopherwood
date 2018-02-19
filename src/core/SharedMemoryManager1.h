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
#ifndef _GOPHERWOOD_CORE_SHAREDMEMORYMANAGER1_H_
#define _GOPHERWOOD_CORE_SHAREDMEMORYMANAGER1_H_

#include "platform.h"

#include "Memory.h"
#include "SharedMemoryContext.h"

namespace Gopherwood {
namespace Internal {

class SharedMemoryManager1 {
public:
    static shared_ptr<SharedMemoryManager1> getInstance(){
        if (!instance)
            instance = shared_ptr<SharedMemoryManager1>(new SharedMemoryManager1());
        return instance;
    }

    shared_ptr<SharedMemoryContext> buildSharedMemoryContext(const char* workDir);

private:
    static shared_ptr<SharedMemoryManager1> instance;
};

}
}

#endif //_GOPHERWOOD_CORE_SHAREDMEMORYMANAGER1_H_
