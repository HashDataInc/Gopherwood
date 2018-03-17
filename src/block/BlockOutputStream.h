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
#ifndef _GOPHERWOOD_BLOCK_BLOCKOUTPUTSTREAM_H_
#define _GOPHERWOOD_BLOCK_BLOCKOUTPUTSTREAM_H_

#include "platform.h"

#include "block/LocalBlockWriter.h"
#include "core/ActiveStatus.h"
#include "common/Memory.h"
#include "oss/oss.h"
#include "OssBlockWriter.h"

namespace Gopherwood {
namespace Internal {
class BlockOutputStream {
public:
    BlockOutputStream(int fd, context ossCtx);

    void setBlockInfo(BlockInfo info);

    int64_t remaining();

    int64_t write(const char *buffer, int64_t length);

    void flush();

    ~BlockOutputStream();
private:
    int64_t getLocalSpaceOffset();

    int mLocalSpaceFD;
    int64_t mBlockSize;
    BlockInfo mBlockInfo;

    shared_ptr<LocalBlockWriter> mLocalWriter;
    shared_ptr<OssBlockWriter> mOssWriter;
};

}
}

#endif //_GOPHERWOOD_BLOCK_BLOCKOUTPUTSTREAM_H_
