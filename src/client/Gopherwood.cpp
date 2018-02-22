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
#include "platform.h"

#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Logger.h"
#include "common/Memory.h"
#include "common/XmlConfig.h"

#include "gopherwood.h"
#include "file/FileSystem.h"
#include "file/InputStream.h"
#include "file/OutputStream.h"

#ifdef __cplusplus
extern "C" {
#endif

using Gopherwood::exception_ptr;
using Gopherwood::Internal::InputStream;
using Gopherwood::Internal::OutputStream;
using Gopherwood::Internal::shared_ptr;
using Gopherwood::Internal::FileSystem;
using Gopherwood::Internal::SetErrorMessage;
using Gopherwood::Internal::SetLastException;

struct GWFileSystemInternalWrapper {
public:
    GWFileSystemInternalWrapper(FileSystem *fs) :
            filesystem(fs) {
    }

    ~GWFileSystemInternalWrapper() {
        delete filesystem;
    }

    FileSystem &getFilesystem() {
        return *filesystem;
    }

private:
    FileSystem *filesystem;
};

struct GWFileInternalWrapper {
public:
    GWFileInternalWrapper() :
            input(true), inputAndOutput(false), inputStream(NULL), outputStream(NULL) {
    }

    ~GWFileInternalWrapper() {
        if (input) {
            delete static_cast<InputStream *>(inputStream);
        } else if (inputAndOutput) {
            delete static_cast<OutputStream *>(outputStream);
            delete static_cast<InputStream *>(inputStream);
        } else {
            delete static_cast<OutputStream *>(outputStream);
        }
    }

    InputStream &getInputStream() {
        if (!input && !inputAndOutput) {
            THROW(Gopherwood::GopherwoodException, "Internal error: file was not opened for read.");
        }

        if (!inputStream) {
            THROW(Gopherwood::GopherwoodIOException, "Inputstream. File is not opened.");
        }

        return *static_cast<InputStream *>(inputStream);
    }

    OutputStream &getOutputStream() {
        if (!output && !inputAndOutput) {
            THROW(Gopherwood::GopherwoodException, "Internal error: file was not opened for write.");
        }

        if (!outputStream) {
            THROW(Gopherwood::GopherwoodIOException, "OutputStream. File is not opened.");
        }

        return *static_cast<OutputStream *>(outputStream);
    }

    bool isInput() const {
        return input;
    }

    bool isOutput() const {
        return output;
    }

    bool isInputAndOutput() const {
        return inputAndOutput;
    }

    void setInput(bool input) {
        this->input = input;
        this->output = !input;
        this->inputAndOutput = !input;
    }

    void setOutput(bool output) {
        this->output = output;
        this->input = !output;
        this->inputAndOutput = !output;
    }

    void setInputAndOutput(bool inputAndOutput) {
        this->inputAndOutput = inputAndOutput;
        this->input = !inputAndOutput;
        this->output = !inputAndOutput;
    }

    void setInputStream(void *stream) {
        this->inputStream = stream;
    }

    void setOutputStream(void *stream) {
        this->outputStream = stream;
    }

private:
    bool input;
    bool output;
    bool inputAndOutput;
    void *inputStream;
    void *outputStream;

};

gopherwoodFS gwCreateContext(char *fileName) {
    gopherwoodFS retVal = NULL;

    FileSystem *fs = NULL;

    fs = new FileSystem(fileName);

    retVal = new GWFileSystemInternalWrapper(fs);

    return retVal;
}

//TODO
tSize gwRead(gopherwoodFS fs, gwFile file, void *buffer, tSize length) {
    return 0;
}

gwFile gwOpenFile(gopherwoodFS fs, const char *fileName, int flags) {
    return NULL;
}

int gwSeek(gopherwoodFS fs, gwFile file, tOffset desiredPos) {
    return -1;
}

int32_t gwWrite(gopherwoodFS fs, gwFile file, const void *buffer, tSize length) {
    return -1;
}

int gwCloseFile(gopherwoodFS fs, gwFile file) {
    return -1;
}

int deleteFile(gopherwoodFS fs, gwFile file) {
    return -1;
}

#ifdef __cplusplus
}
#endif

