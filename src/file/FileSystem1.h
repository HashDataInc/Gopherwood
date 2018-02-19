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
#ifndef _GOPHERWOOD_CORE_FILESYSTEM1_H_
#define _GOPHERWOOD_CORE_FILESYSTEM1_H_

#include "platform.h"

#include "Memory.h"
#include "core/SharedMemoryManager1.h"
#include "core/SharedMemoryContext.h"

namespace Gopherwood {
namespace Internal {

class FileSystem1 {
public:
    FileSystem1(const char *workDir);

    ~FileSystem1();

private:
    const char* workDir;
    shared_ptr<SharedMemoryContext> curSharedMemoryContext;
};

}
}

#endif //_GOPHERWOOD_CORE_FILESYSTEM_H_
