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

#ifndef _GOPHERWOOD_CORE_INPUTSTREAM_H_
#define _GOPHERWOOD_CORE_INPUTSTREAM_H_

#include "FileSystem.h"
#include "FileSystemImpl.h"
#include "FileSystemInter.h"
#include "InputStreamImpl.h"
#include "../common/ExceptionInternal.h"

namespace Gopherwood {
namespace Internal {
class InputStreamInter;
}

/**
 * A input stream used read data from gopherwood.
 */
class InputStream {
public:
    InputStream(FileSystem &fs, const char *fileName, bool verifyChecksum = true);

    ~InputStream();

    /**
     * Open a file to read
     * @param fs gopherwood file system.
     * @param fileName the name of the file to be read.
     * @param verifyChecksum verify the checksum.
     */
//        void open(FileSystem &fs, const char *fileName, bool verifyChecksum = true);
    /**
     * To read data from gopherwood.
     * @param buf the buffer used to filled.
     * @param size buffer size.
     * @return return the number of bytes filled in the buffer, it may less than size.
     */
    int32_t read(char *buf, int32_t size);

    /**
     * To read data from gopherwood, block until get the given size of bytes.
     * @param buf the buffer used to filled.
     * @param size the number of bytes to be read.
     */
    void readFully(char *buf, int64_t size);

    /**
     * Get how many bytes can be read without blocking.
     * @return The number of bytes can be read without blocking.
     */
    int64_t available();

    /**
     * To move the file point to the given position.
     * @param pos the given position.
     */
    void seek(int64_t pos);

    /**
     * To get the current file point position.
     * @return the position of current file point.
     */
    int64_t tell();

    /**
     * Close the stream.
     */
    void close();

    void deleteFile();

private:
    Internal::InputStreamInter *impl;
};

}

#endif /* _GOPHERWOOD_CORE_INPUTSTREAM_H_ */
