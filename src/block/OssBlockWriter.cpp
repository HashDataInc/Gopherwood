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
#include <file/FileSystem.h>
#include "block/OssBlockWriter.h"
#include "common/Configuration.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"

namespace Gopherwood {
namespace Internal {

OssBlockWriter::OssBlockWriter(context ossCtx, int localSpaceFD) :
        mOssContext(ossCtx),
        mLocalSpaceFD(localSpaceFD){
}

void OssBlockWriter::writeBlock(BlockInfo info) {
    int64_t rc = 0;

    int64_t bucketSize = Configuration::LOCAL_BUCKET_SIZE;
    char *buffer = (char*)malloc(bucketSize);

    lseek(mLocalSpaceFD, info.bucketId * bucketSize, SEEK_SET);
    if (rc == -1){
        THROW(GopherwoodIOException,
              "[OssBlockWriter] Local file space seek error!");
    }

    rc = read(mLocalSpaceFD, buffer, bucketSize);
    if (rc != bucketSize){
        THROW(GopherwoodIOException,
              "[OssBlockWriter] Local file space read error!");
    }

    ossObject remoteBlock = ossPutObject(mOssContext,
                                         FileSystem::OSS_BUCKET.c_str(),
                                         info.fileId.toString().c_str(),
                                         false);

    ossWrite(mOssContext, remoteBlock, buffer, bucketSize);
    ossCloseObject(mOssContext, remoteBlock);
}

OssBlockWriter::~OssBlockWriter() {
}

}
}