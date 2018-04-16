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
#include "Configuration.h"

namespace Gopherwood {
namespace Internal {

std::string Configuration::LOCAL_SPACE_FILE("GopherwoodLocal");
std::string Configuration::SHARED_MEMORY_NAME("GopherwoodSharedMem");
std::string Configuration::MANIFEST_FOLDER("/manifest");

int32_t Configuration::NUMBER_OF_BLOCKS = 100;

int64_t Configuration::LOCAL_BUCKET_SIZE = 64 * 1024 * 1024;

int Configuration::MAX_CONNECTION = 1024;

int Configuration::CUR_CONNECTION = 10;

uint32_t Configuration::PRE_ALLOCATE_BUCKET_NUM = 3;

uint32_t Configuration::getCurQuotaSize(){
    return Configuration::NUMBER_OF_BLOCKS/Configuration::CUR_CONNECTION;
}

}
}

