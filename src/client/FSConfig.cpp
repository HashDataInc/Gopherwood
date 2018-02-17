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
#include "../client/FSConfig.h"

#include "../client/SharedMemoryManager.h"

namespace Gopherwood {

namespace Internal {
int64_t SHM_SIZE = 4096; //the size of shared memory
int64_t SIZE_OF_FILE = 20 * 1024; //the size of the SSD file, default is 20GB
int64_t SIZE_OF_BLOCK = 1 * 1024; //the size of the bucket size

int32_t BIT_MAP_SIZE = 40;

const char *BUCKET_PATH_FILE_NAME = "/ssdfile/ssdkv/gopherwood";
//        char *sharedMemoryPath = "/ssdfile/ssdkv/sharedMemory/";
//
//        char *sharedMemoryFileName = "smFile";// the file which save the key of the shared memory

const char *SHARED_MEMORY_PATH_FILE_NAME = "/ssdfile/ssdkv/sharedMemory/smFile";

const char *FILE_LOG_PERSISTENCE_PATH = "/ssdfile/ssdkv/logPersistence/";
unsigned int FILENAME_MAX_LENGTH = 255;
int NUMBER_OF_BLOCKS = 10; //char+(char+long+int(size of file name)+char[255])
unsigned int MIN_QUOTA_SIZE = 2;  // (MIN_QUOTA_SIZE+1)*2<NUMBER_OF_BLOCKS

int QINGSTOR_BUFFER_SIZE = 4 * 1024 * 1024;
int32_t READ_BUFFER_SIZE = SIZE_OF_BLOCK / 4;
int WRITE_BUFFER_SIZE = 8 * 1024 * 1024;

int MAX_PROCESS = 3;  //maximum number of processes running at the same time.
int MAX_QUOTA_SIZE = NUMBER_OF_BLOCKS / MAX_PROCESS;
}

}
