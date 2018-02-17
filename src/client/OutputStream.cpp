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
#include "../client/OutputStream.h"

using namespace Gopherwood::Internal;

namespace Gopherwood {

OutputStream::OutputStream(FileSystem &fs, char *fileName, int flag) {

    if (!fs.impl) {
        THROW(GopherwoodIOException, "FileSystem: not connected.");
    }

    this->impl = new Internal::OutputStreamImpl(fs.impl->filesystem, fileName, flag);
}

OutputStream::~OutputStream() {
    delete impl;
}

void OutputStream::seek(int64_t pos) {
    impl->seek(pos);
}

void OutputStream::write(const char *buf, int64_t size) {
    impl->write(buf, size);
}

void OutputStream::close() {
    impl->close();
}

void OutputStream::deleteFile() {
    impl->deleteFile();
}

}
