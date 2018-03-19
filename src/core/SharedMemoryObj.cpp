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
#include "core/SharedMemoryObj.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Logger.h"

namespace Gopherwood {
namespace Internal {

void ShareMemHeader::enter() {
    if (flags != 0x00) {
        THROW(GopherwoodSharedMemException,
              "[ShareMemHeader::enter] Shared Memory Dirty");
    }
    flags |= 0x01;
}

void ShareMemHeader::exit() {
    flags &= 0xFE;
    if (flags != 0x00) {
        THROW(GopherwoodSharedMemException,
              "[ShareMemHeader::exit] Shared Memory Dirty");
    }
}

void ShareMemBucket::reset() {
    flags = 0;
    fileId.reset();
    fileBlockIndex = InvalidBlockId;
    writeActiveId = InvalidActiveId;
    evictActiveId = InvalidActiveId;
    for (short i=0; i<SMBUCKET_MAX_CONCURRENT_OPEN; i++){
        readActives[i] = InvalidActiveId;
    }
}

void ShareMemBucket::markWrite(int activeId) {
    if (writeActiveId != InvalidActiveId) {
        THROW(GopherwoodSharedMemException,
              "[ShareMemBucket::markWrite] File %lu-%u exceed the max write concurrent num %d",
              fileId.hashcode, fileId.collisionId, 1
        );
    }
    writeActiveId = activeId;
};

void ShareMemBucket::markRead(int activeId) {
    for (int i=0; i<SMBUCKET_MAX_CONCURRENT_OPEN; i++){
        if(readActives[i] == InvalidActiveId){
            readActives[i] = activeId;
            return;
        }
    }
    THROW(GopherwoodSharedMemException,
          "[ShareMemBucket::markRead] File %lu-%u exceed the max  read concurrent num %d",
          fileId.hashcode, fileId.collisionId, SMBUCKET_MAX_CONCURRENT_OPEN
    );
    return;
};

void ShareMemBucket::unmarkWrite(int activeId) {
    if (writeActiveId != activeId) {
        THROW(GopherwoodSharedMemException,
              "[ShareMemBucket::unmarkWrite] File %lu-%u block %d write active id mismatch expect %d, actually is %d",
              fileId.hashcode, fileId.collisionId, fileBlockIndex, activeId, writeActiveId
        );
    }
    writeActiveId = InvalidActiveId;
}
void ShareMemBucket::unmarkRead(int activeId) {
    for (int i=0; i<SMBUCKET_MAX_CONCURRENT_OPEN; i++){
        if(readActives[i] == activeId){
            readActives[i] = InvalidActiveId;
            return;
        }
    }
    THROW(GopherwoodSharedMemException,
          "[ShareMemBucket::unmarkRead] File %lu-%u block %d was not opend by activeId %d ",
          fileId.hashcode, fileId.collisionId, fileBlockIndex, activeId
    );
    return;
}

bool ShareMemBucket::noActiveReadWrite() {
    if (writeActiveId != InvalidActiveId) {
        return false;
    }
    for (int i=0; i<SMBUCKET_MAX_CONCURRENT_OPEN; i++){
        if (readActives[i] != InvalidActiveId){
            return false;
        }
    }
    return true;
}

}
}