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
#ifndef _GOPHERWOOD_CORE_FILESYSTEM_H_
#define _GOPHERWOOD_CORE_FILESYSTEM_H_

#include "platform.h"

#include "common/Memory.h"
#include "common/Unordered.h"
#include "core/ActiveStatusContext.h"
#include "core/SharedMemoryManager.h"
#include "core/SharedMemoryContext.h"
#include "file/File.h"
#include "oss/oss.h"

namespace Gopherwood {
namespace Internal {

class FileSystem {
public:
    static void Format(const char *workDir);

    FileSystem(const char *workDir);

    File* CreateFile(const char *fileName, int flags, bool isWrite);

    File* OpenFile(const char *fileName, int flags, bool isWrite);

    void removeActiveFileStatus(FileId fileId);

    ~FileSystem();

private:
    FileId makeFileId(const std::string filePath);

    void initOssContext();

    int32_t mLocalSpaceFile = -1;
    const char* workDir;
    shared_ptr<SharedMemoryContext> mSharedMemoryContext;
    shared_ptr<ActiveStatusContext> mActiveStatusContext;
    context mOssContext;
};

}
}

#endif //_GOPHERWOOD_CORE_FILESYSTEM_H_
