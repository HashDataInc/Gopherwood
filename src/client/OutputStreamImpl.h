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
#ifndef _GOPHERWOOD_CORE_OUTPUTSTREAMIMPL_H_
#define _GOPHERWOOD_CORE_OUTPUTSTREAMIMPL_H_

#include <iostream>
#include <memory>

#include "../client/OutputStream.h"
#include "../client/OutputStreamInter.h"
#include "Exception.h"
#include "Logger.h"

namespace Gopherwood {
namespace Internal {
class OutputStreamImpl: public OutputStreamInter {

public:

    OutputStreamImpl(std::shared_ptr<FileSystemInter> fs, char *fileName, int flag);

    ~OutputStreamImpl();

    /**
     * To create or append a file.
     * @param fs gopherwood file system.
     * @param fileName the file name.
     * @param flag creation flag, can be Create, Append or Create|Overwrite.
     */
//            void open(std::shared_ptr<FileSystemInter> fs, char *fileName, int flag);
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

    string toString();

    void setError(const exception_ptr &error);

    void seek(int64_t pos);

    void deleteFileBucket(int64_t pos);

    void deleteFile();

private:
    std::shared_ptr<FileSystemInter> filesystem;
    int32_t cursorBucketID = 0; // the cursor bucket id of the output stream
    int32_t cursorIndex = 0; //the index of the cursorBucketID. status->getBlockIdVector()[cursorIndex] = cursorBucketID
    int64_t cursorOffset = 0; // the cursor offset of the output stream
    string fileName;
    std::shared_ptr<FileStatus> status;

private:
//            void createFile(char *fileName);

    void checkStatus(int64_t pos);

    void seekInternal(int64_t pos);

    void writeInternal(char *buf, int64_t size);

    int64_t getRemainLength();

    void seekToNextBlock();
};

}
}

#endif //_GOPHERWOOD_CORE_OUTPUTSTREAMIMPL_H_
