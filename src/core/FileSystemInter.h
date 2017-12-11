/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
/********************************************************************
 * 2014 -
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

#ifndef _GOPHERWOOD_CORE_FILESYSTEMINTER_H_
#define _GOPHERWOOD_CORE_FILESYSTEMINTER_H_

#include <string>
#include <vector>
#include <thread>
#include <unordered_map>
#include <memory>
#include "FileStatus.h"


namespace Gopherwood {

    class FileSystem;

    namespace Internal {

        class InputStreamInter;

        class OutputStreamInter;

        class FileSystemInter;

        struct FileSystemWrapper {
        public:
            FileSystemWrapper(std::shared_ptr<FileSystemInter> fs) :
                    filesystem(fs) {
            }

            std::shared_ptr<FileSystemInter> filesystem;
        };

        class FileSystemInter {
        public:
            /**
             * Destroy a FileSystemInter instance
             */
            virtual ~FileSystemInter() {
            }


            /**
             * check whether the ssd file exist or not
             * @return 1 if exist, -1 otherwise
             */
            virtual int32_t checkSSDFile()=0;




            /**
             * rebuild the FileStatus from the log file
             * @param fileName  the file name
             * @return the file status of the file
             */
            virtual unordered_map<string, std::shared_ptr<FileStatus>> rebuildFileStatusFromLog(char *fileName)=0;
            virtual unordered_map<string, std::shared_ptr<FileStatus>> catchUpFileStatusFromLog(int64_t logOffset)=0;

            virtual bool checkFileExist(char *fileName)=0;

            virtual void createFile(char *fileName)=0;

            virtual void acquireNewBlock(char *fileName)=0;

            virtual int64_t getTheEOFOffset(const char *fileName)=0;

            virtual std::shared_ptr<FileStatus> getFileStatus(const char *fileName)=0;

            virtual int32_t readDataFromBucket(char *buf, int32_t size)=0;

            virtual void writeDataToBucket(char *buf, int64_t size)=0;

            virtual int32_t fsSeek(int64_t offset, int whence)=0;

            virtual void closeFile(char *fileName)=0;
            virtual void stopSystem() = 0;
            virtual std::shared_ptr<FileStatus> getFileStatus(char *fileName)=0;

        };

    }
}

#endif //_GOPHERWOOD_CORE_FILESYSTEMINTER_H_
