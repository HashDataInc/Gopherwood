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

#include "common/Configuration.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Logger.h"
#include "common/Memory.h"

#include "gopherwood.h"
#include "file/FileSystem.h"

#ifdef __cplusplus
extern "C" {
#endif

using Gopherwood::exception_ptr;
using Gopherwood::Internal::Configuration;
using Gopherwood::Internal::InputStream;
using Gopherwood::Internal::BlockOutputStream;
using Gopherwood::Internal::shared_ptr;
using Gopherwood::Internal::File;
using Gopherwood::Internal::FileSystem;
using Gopherwood::Internal::SetErrorMessage;
using Gopherwood::Internal::SetLastException;

struct GWFileSystemInternalWrapper {
public:
    explicit GWFileSystemInternalWrapper(FileSystem *fs) :
            __filesystem(fs) {
    }

    ~GWFileSystemInternalWrapper() {
        delete __filesystem;
    }

    FileSystem &getFilesystem() {
        return *__filesystem;
    }

private:
    FileSystem *__filesystem;
};

struct GWFileInternalWrapper {
public:
    explicit GWFileInternalWrapper(File *file) :
            __file(file) {
    }

    ~GWFileInternalWrapper() {
        delete __file;
    }

    File &getFile() {
        return *__file;
    }

private:
    File *__file;
};

static void handleException(const Gopherwood::exception_ptr &error) {
    try {
        std::string buffer;
        Gopherwood::rethrow_exception(error);
    } catch (const Gopherwood::GopherwoodInvalidParmException &) {
        std::string buffer;
        LOG(
                Gopherwood::Internal::LOG_ERROR,
                "Handle Gopherwood Invalid Parameter Exception: %s",
                Gopherwood::Internal::GetExceptionDetail(error, buffer));
        errno = EINVALIDPARM;
    } catch (const Gopherwood::GopherwoodNotImplException &) {
        std::string buffer;
        LOG(
                Gopherwood::Internal::LOG_ERROR,
                "Handle Gopherwood Function Not Implemented Exception: %s",
                Gopherwood::Internal::GetExceptionDetail(error, buffer));
        errno = ENOTIMPL;
    } catch (const Gopherwood::GopherwoodSharedMemException &) {
        std::string buffer;
        LOG(
                Gopherwood::Internal::LOG_ERROR,
                "Handle Gopherwood Shared Memory Exception: %s",
                Gopherwood::Internal::GetExceptionDetail(error, buffer));
        errno = ESHRMEM;
    } catch (const Gopherwood::GopherwoodSyncException &) {
        std::string buffer;
        LOG(
                Gopherwood::Internal::LOG_ERROR,
                "Handle Gopherwood Sync Exception: %s",
                Gopherwood::Internal::GetExceptionDetail(error, buffer));
        errno = ESYNC;
    } catch (const Gopherwood::GopherwoodOSSException &) {
        std::string buffer;
        LOG(
                Gopherwood::Internal::LOG_ERROR,
                "Handle Gopherwood OSS Exception: %s",
                Gopherwood::Internal::GetExceptionDetail(error, buffer));
        errno = EOSS;
    } catch (const Gopherwood::GopherwoodException &) {
        std::string buffer;
        LOG(
                Gopherwood::Internal::LOG_ERROR,
                "Handle Gopherwood exception: %s",
                Gopherwood::Internal::GetExceptionDetail(error, buffer));
        errno = EGOPHERWOOD;
    } catch (const std::bad_alloc &) {
        LOG(Gopherwood::Internal::LOG_ERROR, "Out of memory");
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (const std::exception &) {
        std::string buffer;
        LOG(
                Gopherwood::Internal::LOG_ERROR,
                "Unexpected exception: %s",
                Gopherwood::Internal::GetExceptionDetail(error, buffer));
        errno = EINTERNAL;
    }
}

gopherwoodFS gwCreateContext(char *workDir, GWContextConfig *config) {
    LOG(Gopherwood::Internal::INFO, "------------------gwCreateContext start------------------");
    PARAMETER_ASSERT(workDir && strlen(workDir) > 0, NULL, EINVAL);

    gopherwoodFS retVal = NULL;

    if (config != NULL) {
        PARAMETER_ASSERT(config->numBlocks > 0, NULL, EINVAL);
        PARAMETER_ASSERT(config->blockSize > 0, NULL, EINVAL);
        PARAMETER_ASSERT(config->numPreDefinedConcurrency > 0, NULL, EINVAL);

        Configuration::NUMBER_OF_BLOCKS = config->numBlocks;
        Configuration::LOCAL_BUCKET_SIZE = config->blockSize;
        Configuration::CUR_CONNECTION = config->numPreDefinedConcurrency;
    }

    try {
        FileSystem *fs = new FileSystem(workDir);
        retVal = new GWFileSystemInternalWrapper(fs);
    } catch (...) {
        SetLastException(Gopherwood::current_exception());
        handleException(Gopherwood::current_exception());
    }
    return retVal;
}

void gwFormatContext(char *workDir) {
    LOG(Gopherwood::Internal::INFO, "------------------gwFormatContext start------------------");

    try {
        FileSystem::Format(workDir);
    } catch (...) {
        SetLastException(Gopherwood::current_exception());
        handleException(Gopherwood::current_exception());
    }
}

gwFile gwOpenFile(gopherwoodFS fs, const char *fileName, int flags) {
    LOG(Gopherwood::Internal::INFO, "------------------gwOpenFile start------------------");

    gwFile retVal = NULL;
    File *file;

    try {
        bool isWrite = (flags & GW_RDWR) || (flags & GW_WRONLY);
        if (flags & GW_CREAT) {
            file = fs->getFilesystem().CreateFile(fileName, flags, isWrite);
        } else {
            file = fs->getFilesystem().OpenFile(fileName, flags, isWrite);
        }

        retVal = new GWFileInternalWrapper(file);
    } catch (...) {
        SetLastException(Gopherwood::current_exception());
        handleException(Gopherwood::current_exception());
    }

    LOG(Gopherwood::Internal::INFO, "------------------gwOpenFile end------------------");

    return retVal;
}

tSize gwRead(gopherwoodFS fs, gwFile file, void *buffer, tSize length) {
    LOG(Gopherwood::Internal::INFO, "------------------gwRead start------------------");
    try {
        tSize bytesRead = file->getFile().read(static_cast<char *>(buffer), length);
        return bytesRead;
    } catch (...) {
        SetLastException(Gopherwood::current_exception());
        handleException(Gopherwood::current_exception());
    }

    return -1;
}

int64_t gwSeek(gopherwoodFS fs, gwFile file, tOffset desiredPos, int mode) {
    LOG(Gopherwood::Internal::INFO, "------------------gwSeek start------------------");
    try {
        return file->getFile().seek(desiredPos, mode);
    } catch (...) {
        SetLastException(Gopherwood::current_exception());
        handleException(Gopherwood::current_exception());
    }

    return -1;
}

int32_t gwWrite(gopherwoodFS fs, gwFile file, const void *buffer, tSize length) {
    LOG(Gopherwood::Internal::INFO, "------------------gwWrite start------------------");
    try {
        file->getFile().write(static_cast<const char *>(buffer), length);
        return length;
    } catch (...) {
        SetLastException(Gopherwood::current_exception());
        handleException(Gopherwood::current_exception());
    }

    return -1;
}

int gwFlush(gopherwoodFS fs, gwFile file) {
    LOG(Gopherwood::Internal::INFO, "------------------gwFlush start------------------");
    try {
        file->getFile().flush();
        return 0;
    } catch (...) {
        SetLastException(Gopherwood::current_exception());
        handleException(Gopherwood::current_exception());
    }

    return -1;
}

int gwCloseFile(gopherwoodFS fs, gwFile file) {
    LOG(Gopherwood::Internal::INFO, "------------------gwCloseFile start------------------");
    try {
        fs->getFilesystem().CloseFile(file->getFile());
        delete file;
        file = NULL;
        return 0;
    } catch (...) {
        SetLastException(Gopherwood::current_exception());
        handleException(Gopherwood::current_exception());
    }

    return -1;
}

int gwDeleteFile(gopherwoodFS fs, char *filePath) {
    LOG(Gopherwood::Internal::INFO, "------------------gwDeleteFile start------------------");
    try {
        fs->getFilesystem().DeleteFile(filePath);
        return 0;
    } catch (...) {
        SetLastException(Gopherwood::current_exception());
        handleException(Gopherwood::current_exception());
    }

    return -1;
}

int gwDestroyContext(gopherwoodFS fs) {
    LOG(Gopherwood::Internal::INFO, "------------------gwDestroyContext start------------------");
    try {
        if (fs) {
            delete fs;
            fs = NULL;
        }
        return 0;
    } catch (...) {
        delete fs;
        SetLastException(Gopherwood::current_exception());
        handleException(Gopherwood::current_exception());
    }

    return -1;
}

int gwCancelFile(gopherwoodFS fs, gwFile file)
{
	return 0;
}

int gwStatFile(gopherwoodFS fs, gwFile file, GWFileInfo* fi)
{
	LOG(Gopherwood::Internal::INFO, "------------------gwStatFile start------------------");
	int retVal = 0;
	try{
		fi->fileSize = file->getFile().getFileSize();
	}catch (...) {
		retVal = -1;
		SetLastException(Gopherwood::current_exception());
		handleException(Gopherwood::current_exception());
	}

	return 0;
}

#ifdef __cplusplus
}
#endif

