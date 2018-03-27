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
#ifndef _GOPHERWOOD_CORE_FILE_H_
#define _GOPHERWOOD_CORE_FILE_H_

#include "platform.h"

#include "core/ActiveStatus.h"
#include "common/Memory.h"
#include "file/OutputStream.h"
#include "file/InputStream.h"
#include "oss/oss.h"

namespace Gopherwood {
namespace Internal {

#define OPEN_TYPE_MASK 0x00000003

class File {
public:
    File(FileId id, std::string fileName, int flags, int fd, shared_ptr<ActiveStatus> status);

    int64_t read(char *buffer, int64_t length);

    void write(const char *buffer, int64_t length);

    void flush();

    int64_t seek(int64_t pos, int mode);

    void close();

    int64_t remaining();

    FileId getFileId();

    int64_t getFileSize() {return mStatus->getEof();}

    ~File();

private:
    FileId id;
    std::string name;
    std::string nameDigest;
    int mFlags;
    int localFD;
    shared_ptr<ActiveStatus> mStatus;
    shared_ptr<OutputStream> mOutStream;
    shared_ptr<InputStream> mInStream;
};

}
}

#endif //_GOPHERWOOD_CORE_FILE_H_
