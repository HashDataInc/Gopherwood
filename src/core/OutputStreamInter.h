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

#ifndef _GOPHERWOOD_CORE_OUTPUTSTREAMINTER_H_
#define _GOPHERWOOD_CORE_OUTPUTSTREAMINTER_H_


#include "OutputStream.h"
#include "FileSystemInter.h"


namespace Gopherwood {
    namespace Internal {

/**
 * A output stream used to write data to hdfs.
 */
        class OutputStreamInter {
        public:
            virtual ~OutputStreamInter() {
            }

            /**
         * To create or append a file.
         * @param fs gopherwood file system.
         * @param fileName the file name.
         * @param flag creation flag, can be Create, Append or Create|Overwrite.
         */
//            virtual void open(std::shared_ptr<FileSystemInter> fs,  char *fileName, int flag);

            /**
             * To write data to file.
             * @param buf the data used to write.
             * @param size the data size.
             */
            virtual void write(const char *buf, int64_t size)=0;

            /**
             * Flush all data in buffer and waiting for ack.
             * Will block until get all acks.
             */
            virtual void flush()=0;

            /**
             * return the current file length.
             * @return current file length.
             */
            virtual int64_t tell()=0;

            /**
             * the same as flush right now.
             */
            virtual void sync()=0;

            /**
             * close the stream.
             */
            virtual void close()=0;


            /**
            * To move the file point to the given position.
            * @param pos the given position.
            */
            virtual void seek(int64_t pos)=0;

            virtual string toString()=0;

            virtual void setError(const exception_ptr &error) = 0;

            virtual void deleteFile()=0;

            virtual std::shared_ptr<FileStatus> getFileStatus()=0;

        };

    }
}

#endif //_GOPHERWOOD_CORE_OUTPUTSTREAMINTER_H_
