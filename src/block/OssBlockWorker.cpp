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
#include <oss/oss.h>
#include "block/OssBlockWorker.h"
#include "common/Configuration.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "file/FileSystem.h"

namespace Gopherwood {
namespace Internal {

OssBlockWorker::OssBlockWorker(ossContext ossCtx, int localSpaceFD) :
        mOssContext(ossCtx),
        mLocalSpaceFD(localSpaceFD){
}

void OssBlockWorker::writeBlock(BlockInfo info) {
    int64_t rc = 0;

    int64_t bucketSize = Configuration::LOCAL_BUCKET_SIZE;
    char *buffer = (char*)malloc(info.dataSize);

    rc = lseek(mLocalSpaceFD, info.bucketId * bucketSize, SEEK_SET);
    if (rc == -1){
        THROW(GopherwoodIOException,
              "[OssBlockWorker] Local file space seek error!");
    }

    rc = read(mLocalSpaceFD, buffer, info.dataSize);
    if (rc != info.dataSize){
        THROW(GopherwoodIOException,
              "[OssBlockWorker] Local file space read error!");
    }

    ossObject remoteBlock = ossPutObject(mOssContext,
                                         FileSystem::OSS_BUCKET.c_str(),
                                         getOssObjectName(info).c_str(),
                                         false);

    ossWrite(mOssContext, remoteBlock, buffer, info.dataSize);
    ossCloseObject(mOssContext, remoteBlock);
    free(buffer);
    buffer = NULL;
}

void OssBlockWorker::readBlock(BlockInfo info) {
    int64_t rc = 0;

    int64_t bucketSize = Configuration::LOCAL_BUCKET_SIZE;

    /* get object info */
    ossHeadResult *headResult = ossHeadObject(mOssContext,
                                              FileSystem::OSS_BUCKET.c_str(),
                                              getOssObjectName(info).c_str());
    if (!headResult) {
        THROW(GopherwoodIOException, "OssBlockWorker head object failed!");
    }

    /* malloc buffer based on the object size */
    int64_t objectSize = headResult->content_length;
    char *buffer = (char*)malloc(objectSize);

    /* get object */
    ossObject remoteBlock = ossGetObject(mOssContext,
                                         FileSystem::OSS_BUCKET.c_str(),
                                         getOssObjectName(info).c_str(),
                                         0,
                                         objectSize - 1);
    if (!remoteBlock) {
        THROW(GopherwoodIOException, "OssBlockWorker read failed, reader object is null!");
    }


    int64_t bytesToRead = objectSize;
    int64_t bytesRead = 0;
    int64_t offset = 0;

    do {
        bytesRead = ossRead(mOssContext, remoteBlock, buffer + offset, bytesToRead);
        bytesToRead -= bytesRead;
        offset += bytesRead;
    } while (bytesRead > 0 && bytesToRead > 0);

    ossCloseObject(mOssContext, remoteBlock);
    if (offset != objectSize || bytesToRead !=0) {
        THROW(GopherwoodIOException,
              "[OssBlockWorker] Remote file size mismatch, expect %ld, but got %ld!",
              objectSize,
              bytesRead);
    }

    rc = lseek(mLocalSpaceFD, info.bucketId * bucketSize, SEEK_SET);
    if (rc == -1){
        THROW(GopherwoodIOException,
              "[OssBlockWorker] Local file space seek error!");
    }

    rc = write(mLocalSpaceFD, buffer, objectSize);
    if (rc != objectSize){
        THROW(GopherwoodIOException,
              "[OssBlockWorker] Local file space read error!");
    }
    free(headResult);
    free(buffer);
    buffer = NULL;
}

void OssBlockWorker::deleteBlock(BlockInfo info) {
    int rc = ossDeleteObject(mOssContext, FileSystem::OSS_BUCKET.c_str(), getOssObjectName(info).c_str());
    if (rc == -1){
        THROW(GopherwoodIOException,
              "[OssBlockWorker] OSS file delete error!");
    }
}

std::string OssBlockWorker::getOssObjectName(BlockInfo blockInfo){
    std::stringstream ss;
    char hostname[1024];
    gethostname(hostname, 1024);
    ss << "gopherwood/" << hostname << '/' << blockInfo.fileId.toString() << '/'
       << blockInfo.blockId;
    return ss.str();
}

OssBlockWorker::~OssBlockWorker() {
}

}
}