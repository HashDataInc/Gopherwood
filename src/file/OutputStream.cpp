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
#include "file/OutputStream.h"

namespace Gopherwood {
namespace Internal {

OutputStream::OutputStream(int fd, shared_ptr<ActiveStatus> status) :
        mLocalSpaceFD(fd), status(status){
    mPos = -1;
    blockOutputStream = shared_ptr<BlockOutputStream>(new BlockOutputStream(mLocalSpaceFD));
}

void OutputStream::write(const char *buffer, int64_t length) {
    int64_t bytesToWrite = length;
    int64_t bytesWritten = 0;
    bool needUpdate = false;

    /* update OutputStream file level position */
    int64_t statusPos = status->getPosition();
    if(mPos != statusPos){
        needUpdate = true;
        mPos = statusPos;
    }

    /* write the buffer, switch target block if needed */
    while(bytesToWrite > 0)
    {
        /* update BlockOutputStream, flush previous cached data
         * and switch to target block id & offset */
        if (needUpdate) {
            updateBlockStream();
            needUpdate = false;
        }

        /* write to target block */
        int64_t written;
        if (bytesToWrite <= blockOutputStream->remaining()) {
            written = blockOutputStream->write(buffer + bytesWritten, bytesToWrite);
        }else {
            written = blockOutputStream->write(buffer + bytesWritten, blockOutputStream->remaining());
            needUpdate = true;
        }

        /* update statistics */
        bytesToWrite -= written;
        bytesWritten += written;
        mPos += written;
        status->setPosition(mPos);
    }
}

void OutputStream::updateBlockStream(){
    /* TODO: Implement this once we make BlockOutput stream a buffered stream */
    blockOutputStream->flush();

    /* Update the BlockInfo of the BlockOutputStream */
    BlockInfo info = status->getCurBlockInfo();
    blockOutputStream->setBlockInfo(info.id, info.offset);
}

OutputStream::~OutputStream(){

}


}
}
