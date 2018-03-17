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
#include "block/BlockInputStream.h"
#include "common/Configuration.h"
#include "common/Logger.h"

namespace Gopherwood {
namespace Internal {

BlockInputStream::BlockInputStream(int fd, context ossCtx) : mLocalSpaceFD(fd) {
    mLocalReader = shared_ptr<LocalBlockReader>(new LocalBlockReader(fd));
    mOssReader = shared_ptr<OssBlockReader>(new OssBlockReader(ossCtx));
    mBlockSize = Configuration::LOCAL_BLOCK_SIZE;
}

void BlockInputStream::setBlockInfo(BlockInfo info) {
    LOG(INFO, "[BlockInputStream] Set BlockInfo, new blockId=%d, new blockOffset=%ld, %s",
        info.blockId, info.offset, info.isLocal?"local":"remote");
    mBlockInfo = info;
}

int64_t BlockInputStream::remaining() {
    return mBlockSize - mBlockInfo.offset;
}

int64_t BlockInputStream::read(char *buffer, int64_t length){
    int64_t read = -1;

    if (mBlockInfo.isLocal)
    {
        if (mLocalReader->getCurOffset() != getLocalSpaceOffset())
        {
            mLocalReader->seek(getLocalSpaceOffset());
        }
        LOG(INFO, "[BlockInputStream] Read from local space, blockId=%d, offset=%ld, length=%ld",
            mBlockInfo.blockId, mBlockInfo.offset, length);
        read = mLocalReader->readLocal(buffer, length);
    } else{
        /* Read from OSS */
    }

    mBlockInfo.offset += read;
    assert(mBlockInfo.offset<=mBlockSize);

    return read;
}

void BlockInputStream::flush()
{

}

int64_t BlockInputStream::getLocalSpaceOffset(){
    return mBlockInfo.blockId * mBlockSize + mBlockInfo.offset;
}

BlockInputStream::~BlockInputStream() {

}

}
}