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
#ifndef _GOPHERWOOD_CORE_OUTPUTSTREAM_H_
#define _GOPHERWOOD_CORE_OUTPUTSTREAM_H_

#include "FileSystem.h"
#include "OutputStreamImpl.h"
#include "Exception.h"
#include "ExceptionInternal.h"

namespace Gopherwood {

/**
 * Use the CreateFlag as follows:
 * <ol>
 * <li> CREATE - to create a file if it does not exist,
 * else throw FileAlreadyExists.</li>
 * <li> APPEND - to append to a file if it exists,
 * else throw FileNotFoundException.</li>
 * <li> OVERWRITE - to truncate a file if it exists,
 * else throw FileNotFoundException.</li>
 * <li> CREATE|APPEND - to create a file if it does not exist,
 * else append to an existing file.</li>
 * <li> CREATE|OVERWRITE - to create a file if it does not exist,
 * else overwrite an existing file.</li>
 * <li> SyncBlock - to force closed blocks to the disk device.
 * In addition {@link OutputStream::sync()} should be called after each write,
 * if true synchronous behavior is required.</li>
 * </ol>
 *
 * Following combination is not valid and will result in
 * {@link InvalidParameter}:
 * <ol>
 * <li> APPEND|OVERWRITE</li>
 * <li> CREATE|APPEND|OVERWRITE</li>
 * </ol>
 */
//    enum CreateFlag {
//        Create = 0x01, Overwrite = 0x02, Append = 0x04, SyncBlock = 0x08
//    };

    enum CreateFlag {
        ReadOnly = 0x01, WriteOnly = 0x02, ReadWrite = 0x04
    };


    namespace Internal {
        class OutputStreamInter;
    }

/**
 * A output stream used to write data to hdfs.
 */
    class OutputStream {
    public:
        /**
         * Construct a new OutputStream.
         */
        OutputStream(FileSystem &fs, char *fileName, int flag = ReadWrite);

        /**
         * Destroy a OutputStream instance.
         */
        ~OutputStream();

        /**
         * To create or append a file.
         * @param fs gopherwood file system.
         * @param fileName the file name.
         * @param flag creation flag, can be Create, Append or Create|Overwrite.
         */
//        void open(FileSystem &fs, const char *fileName, int flag = Create);

        /**
         * To write data to file.
         * @param buf the data used to write.
         * @param size the data size.
         */
        void write(const char *buf, int64_t size);

        /**
         * Flush all data in buffer and waiting for ack.
         * Will block until get all acks.
         */
        void flush();

        /**
         * return the current file length.
         * @return current file length.
         */
        int64_t tell();

        /**
         * the same as flush right now.
         */
        void sync();

        /**
         * close the stream.
         */
        void close();

        int64_t seek(int64_t pos);


        void deleteFile();

        std::shared_ptr<FileStatus> getFileStatus();

    private:
        Internal::OutputStreamInter *impl;
    };

}

#endif /* _GOPHERWOOD_CORE_OUTPUTSTREAM_H_ */
