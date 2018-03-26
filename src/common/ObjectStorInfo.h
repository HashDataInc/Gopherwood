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
#ifndef GOPHERWOOD_COMMON_OBJECTSTORINFO_H
#define GOPHERWOOD_COMMON_OBJECTSTORINFO_H

#include "platform.h"

#include <string>

namespace Gopherwood {
namespace Internal {

struct ObjectStorInfo {
    std::string object_stor_type;
    std::string liboss_access_key_id;
    std::string liboss_secret_access_key;
    std::string liboss_appid = " ";
    std::string liboss_zone;
    int64_t liboss_write_buffer = 8 << 20;
    int64_t liboss_read_buffer = 32 << 20;

    std::string qs_access_key_id;
    std::string qs_secret_access_key;
    std::string qs_zone;
    int64_t qs_write_buffer = 8 << 20;
    int64_t qs_read_buffer = 32 << 20;

    std::string s3_access_key_id;
    std::string s3_secret_access_key;
    std::string s3_zone;
    int64_t s3_write_buffer = 8 << 20;
    int64_t s3_read_buffer = 32 << 20;

    std::string txcos_access_key_id;
    std::string txcos_secret_access_key;
    std::string txcos_appid;
    std::string txcos_zone;
    int64_t txcos_write_buffer = 8 << 20;
    int64_t txcos_read_buffer = 32 << 20;

    std::string alioss_access_key_id;
    std::string alioss_secret_access_key;
    std::string alioss_zone;
    int64_t alioss_write_buffer = 8 << 20;
    int64_t alioss_read_buffer = 32 << 20;
};

}
}

#endif //GOPHERWOOD_COMMON_OBJECTSTORINFO_H
