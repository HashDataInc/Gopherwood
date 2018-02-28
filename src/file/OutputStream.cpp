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
    pos = 0;
    blockOutputStream = shared_ptr<BlockOutputStream>(new BlockOutputStream(mLocalSpaceFD));
}

void OutputStream::write(const char *buffer, int64_t length) {
    int64_t bytesToWrite = length;
    int64_t curPos = 0;
    bool needUpdate = true;

    /* write the buffer, switch target block if needed */
    while(bytesToWrite > 0)
    {
        /* update BlockOutputStream, flush previous cached data
         * and switch to target block id & offset */
        if (needUpdate) {
            updateBlockStream();
            needUpdate = false;
        }

        int64_t written;
        if (bytesToWrite <= blockOutputStream->remaining()) {
            written = blockOutputStream->write(buffer + curPos, bytesToWrite);
            bytesToWrite -= written;
        }else {
            written = blockOutputStream->write(buffer + curPos, blockOutputStream->remaining());
            bytesToWrite -= written;
            needUpdate = true;
        }
    }
}

void OutputStream::updateBlockStream(){
    blockOutputStream->flush();

    /* get current block info */
    BlockInfo info = status->getCurBlockInfo();
    blockOutputStream->setPosition(info.id, info.offset);
}

OutputStream::~OutputStream(){

}


}
}
