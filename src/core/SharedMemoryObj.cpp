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
    usageCount = 0;
    fileBlockIndex = InvalidBlockId;
    evictLoadActiveId = InvalidActiveId;
    for (int16_t i = 0; i < SMBUCKET_MAX_CONCURRENT_OPEN; i++) {
        activeInfos[i].reset();
    }
}

void ShareMemBucket::markWrite(int activeId) {
    for (int i = 0; i < SMBUCKET_MAX_CONCURRENT_OPEN; i++) {
        if (activeInfos[i].activeId == InvalidActiveId) {
            activeInfos[i].activeId = activeId;
            activeInfos[i].setWrite();
            return;
        }
    }
    THROW(GopherwoodSharedMemException,
          "[ShareMemBucket::markRead] File %lu-%u exceed the max activate concurrent num %d",
          fileId.hashcode, fileId.collisionId, SMBUCKET_MAX_CONCURRENT_OPEN
    );
    return;
};

void ShareMemBucket::markRead(int activeId) {
    for (int i = 0; i < SMBUCKET_MAX_CONCURRENT_OPEN; i++) {
        if (activeInfos[i].activeId == InvalidActiveId) {
            activeInfos[i].activeId = activeId;
            activeInfos[i].setRead();
            return;
        }
    }
    THROW(GopherwoodSharedMemException,
          "[ShareMemBucket::markRead] File %lu-%u exceed the max activate concurrent num %d",
          fileId.hashcode, fileId.collisionId, SMBUCKET_MAX_CONCURRENT_OPEN
    );
    return;
};

void ShareMemBucket::unmarkWrite(int16_t activeId) {
    for (int i = 0; i < SMBUCKET_MAX_CONCURRENT_OPEN; i++) {
        if (activeInfos[i].activeId == activeId) {
            activeInfos[i].reset();
            return;
        }
    }
    THROW(GopherwoodSharedMemException,
          "[ShareMemBucket::unmarkRead] File %lu-%u block %d was not opend by activeId %d ",
          fileId.hashcode, fileId.collisionId, fileBlockIndex, activeId
    );
    return;
}

void ShareMemBucket::unmarkRead(int16_t activeId) {
    for (int i = 0; i < SMBUCKET_MAX_CONCURRENT_OPEN; i++) {
        if (activeInfos[i].activeId == activeId) {
            activeInfos[i].reset();
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
    for (int i = 0; i < SMBUCKET_MAX_CONCURRENT_OPEN; i++) {
        if (activeInfos[i].activeId != InvalidActiveId) {
            return false;
        }
    }
    return true;
}

}
}