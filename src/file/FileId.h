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
#ifndef _GOPHERWOOD_CORE_FILEID_H_
#define _GOPHERWOOD_CORE_FILEID_H_

#include "platform.h"

#include "common/Unordered.h"
#include <sstream>

namespace Gopherwood {
namespace Internal {

struct FileId {

    size_t hashcode;
    uint32_t collisionId;

    FileId() : hashcode(0), collisionId(0) {};

    void reset() {
        hashcode = -1;
        collisionId = -1;
    }
    std::string toString()
    {
        std::stringstream ss;
        ss << hashcode << collisionId;
        std::string res = ss.str();
        return res;
    }

    bool operator==(const FileId& rhs) const {
        if (hashcode != rhs.hashcode ||
            collisionId != rhs.collisionId) {
            return false;
        } else {
            return true;
        }
    }
};

}
}

#endif //_GOPHERWOOD_CORE_FILEID_H_
