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
#include "../client/InputStream.h"

using namespace Gopherwood::Internal;

namespace Gopherwood {

InputStream::InputStream(FileSystem &fs, const char *fileName, bool verifyChecksum) {

    if (!fs.impl) {
        THROW(GopherwoodIOException, "FileSystem: not connected.");
    }

    impl = new Internal::InputStreamImpl(fs.impl->filesystem, fileName, verifyChecksum);

}

InputStream::~InputStream() {
    delete impl;
}

int32_t InputStream::read(char *buf, int32_t size) {
    return impl->read(buf, size);
}

//    void InputStream::open(FileSystem &fs, const char *fileName, bool verifyChecksum = true){
//        if (!fs.impl) {
//            THROW(GopherwoodException, "FileSystem: not connected.");
//        }
//
//        impl->open(fs.impl->filesystem, fileName, verifyChecksum);
//
//    }

void InputStream::seek(int64_t pos) {
    impl->seek(pos);
}

void InputStream::close() {
    impl->close();
}

void InputStream::deleteFile() {
    impl->deleteFile();
}
}

