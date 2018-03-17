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
#include "file/InputStream.h"

namespace Gopherwood {
namespace Internal {

InputStream::InputStream(int fd, shared_ptr<ActiveStatus> status, context ossCtx) :
        mLocalSpaceFD(fd), mStatus(status){
    mPos = -1;
    mBlockInputStream = shared_ptr<BlockInputStream>(new BlockInputStream(mLocalSpaceFD, ossCtx));
}

void InputStream::updateBlockStream(){
    /* TODO: Implement this once we make BlockOutput stream a buffered stream */
    mBlockInputStream->flush();

    /* Update the BlockInfo of the BlockOutputStream */
    mBlockInputStream->setBlockInfo(mStatus->getCurBlockInfo());
}

void InputStream::read(char *buffer, int64_t length) {
    int64_t bytesToRead = length;
    int64_t bytesRead = 0;
    bool needUpdate = false;

    /* update OutputStream file level position */
    int64_t statusPos = mStatus->getPosition();
    if(mPos != statusPos){
        needUpdate = true;
        mPos = statusPos;
    }

    /* write the buffer, switch target block if needed */
    while(bytesToRead > 0)
    {
        /* update BlockOutputStream, flush previous cached data
         * and switch to target block id & offset */
        if (needUpdate) {
            updateBlockStream();
            needUpdate = false;
        }

        /* write to target block */
        int64_t read;
        if (bytesToRead <= mBlockInputStream->remaining()) {
            read = mBlockInputStream->read(buffer + bytesRead, bytesToRead);
        }else {
            read = mBlockInputStream->read(buffer + bytesRead, mBlockInputStream->remaining());
            needUpdate = true;
        }

        /* update statistics */
        bytesToRead -= read;
        bytesRead += read;
        mPos += read;
        mStatus->setPosition(mPos);
    }
}

void InputStream::close() {

}

InputStream::~InputStream(){
    
}

}
}
