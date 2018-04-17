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
#include "file/FileSystem.h"

namespace Gopherwood {
namespace Internal {

BlockInputStream::BlockInputStream(int fd) : mLocalSpaceFD(fd) {
    mLocalReader = shared_ptr<LocalBlockReader>(new LocalBlockReader(fd));
    mOssWorker = shared_ptr<OssBlockWorker>(new OssBlockWorker(FileSystem::OSS_CONTEXT, fd));
    mBucketSize = Configuration::LOCAL_BUCKET_SIZE;
}

void BlockInputStream::setBlockInfo(BlockInfo info) {
    LOG(DEBUG1, "[BlockInputStream]      |"
              "Set BlockInfo, new bucketId=%d, new blockOffset=%ld, %s",
        info.bucketId, info.offset, info.isLocal ? "local" : "remote");
    mBlockInfo = info;
}

int64_t BlockInputStream::remaining() {
    return mBucketSize - mBlockInfo.offset;
}

int64_t BlockInputStream::read(char *buffer, int64_t length) {
    int64_t read = -1;

    if (mBlockInfo.isLocal) {
        mLocalReader->seek(getLocalSpaceOffset());
        read = mLocalReader->readLocal(buffer, length);
        LOG(DEBUG1, "[BlockInputStream]      |"
                "Read from local space, bucketId=%d, offset=%ld, length=%ld",
            mBlockInfo.bucketId, mBlockInfo.offset, length);
    } else {
        /* Read from OSS */
    }

    mBlockInfo.offset += read;
    assert(mBlockInfo.offset <= mBucketSize);

    return read;
}

void BlockInputStream::flush() {

}

int64_t BlockInputStream::getLocalSpaceOffset() {
    return mBlockInfo.bucketId * mBucketSize + mBlockInfo.offset;
}

BlockInputStream::~BlockInputStream() {

}

}
}