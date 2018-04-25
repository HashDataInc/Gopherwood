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
#include "file/FileSystem.h"
#include "common/Configuration.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Hash.h"
#include "common/OssBuilder.h"
#include <fstream>

namespace Gopherwood {
namespace Internal {

ossContext FileSystem::OSS_CONTEXT = NULL;
std::string FileSystem::OSS_BUCKET = "";

void FileSystem::Format(const char *workDir) {
    std::stringstream ss;
    ss << "exec rm -r " << workDir << "/*";
    system(ss.str().c_str());
    shared_memory_object::remove(Configuration::SHARED_MEMORY_NAME.c_str());
    LOG(DEBUG1, "[FileSystem]            |"
            "Format SharedMemory %s", Configuration::SHARED_MEMORY_NAME.c_str());
}

FileSystem::FileSystem(const char *workDir) :
        workDir(workDir) {
    /* open local space file */
    std::stringstream ss;
    ss << workDir << '/' << Configuration::LOCAL_SPACE_FILE;
    std::string filePath = ss.str();
    if( access(filePath.c_str(), F_OK ) != -1 ) {
        mLocalSpaceFile = open(filePath.c_str(), O_RDWR, 0644);
    } else {
        mLocalSpaceFile = open(filePath.c_str(), O_CREAT | O_RDWR, 0644);
    }

    /* create lock file */
    ss.str("");
    ss << workDir << "/SmLock";
    filePath = ss.str();
    int32_t lockFile = open(filePath.c_str(), O_CREAT | O_RDWR, 0644);

    /* create Manifest log folder */
    ss.str("");
    ss << workDir << Configuration::MANIFEST_FOLDER;
    filePath = ss.str();
    struct stat st = {0};
    if (stat(filePath.c_str(), &st) == -1) {
        mkdir(filePath.c_str(), 0755);
    }

    mSharedMemoryContext = SharedMemoryManager::getInstance()->buildSharedMemoryContext(workDir, lockFile);
    mActiveStatusContext = shared_ptr<ActiveStatusContext>(new ActiveStatusContext(mSharedMemoryContext));

    /* init liboss context */
    initOssContext();
}

FileId FileSystem::makeFileId(const std::string filePath) {
    FileId id;

    /* hash the path to size_t */
    id.hashcode = StringHasher(filePath);

    /* if the hashcode collates with other files,
     * assign an identical collision id
     * TODO: Not implemented, need to check existing files */
    id.collisionId = 0;

    return id;
}

bool FileSystem::exists(const char *fileName) {
    FileId fileId = makeFileId(std::string(fileName));

    std::stringstream ss;
    ss << mSharedMemoryContext->getWorkDir() << Configuration::MANIFEST_FOLDER << "/" << fileId.hashcode << "-"
       << fileId.collisionId;
    std::string manifestFileName = ss.str();
    if (access(manifestFileName.c_str(), F_OK) == -1) {
        return false;
    }
    return true;
}


File *FileSystem::CreateFile(const char *fileName, int flags, bool isWrite) {
    FileId fileId;
    shared_ptr<ActiveStatus> status;

    fileId = makeFileId(std::string(fileName));
    status = mActiveStatusContext->createFileActiveStatus(fileId,
                                                          isWrite,
                                                          flags & GW_SEQACC,
                                                          mLocalSpaceFile);

    LOG(DEBUG1, "[FileSystem]            |"
            "Creating file %s", fileId.toString().c_str());
    std::string name(fileName);
    return new File(fileId, name, flags, mLocalSpaceFile, status);
}

File *FileSystem::OpenFile(const char *fileName, int flags, bool isWrite) {
    FileId fileId;
    shared_ptr<ActiveStatus> status;

    fileId = makeFileId(std::string(fileName));
    status = mActiveStatusContext->openFileActiveStatus(fileId, isWrite, flags & GW_SEQACC, mLocalSpaceFile);

    LOG(DEBUG1, "[FileSystem]            |"
            "Opening file %s", fileId.toString().c_str());
    std::string name(fileName);
    return new File(fileId, name, flags, mLocalSpaceFile, status);
}

void FileSystem::CloseFile(File &file) {
    file.close(false);
}

void FileSystem::DeleteFile(const char *fileName) {
    FileId delFileId = makeFileId(std::string(fileName));
    shared_ptr<ActiveStatus> status;

    /* open file with delete type */
    status = mActiveStatusContext->deleteFileActiveStatus(delFileId, mLocalSpaceFile);

    /* call activeStatus destroy */
    status->close(false);
    status.reset();
}

void FileSystem::initOssContext() {
    OSS_CONTEXT = ossRootBuilder.buildContext();
}

FileSystem::~FileSystem() {
    if (mLocalSpaceFile > 0) {
        close(mLocalSpaceFile);
        mLocalSpaceFile = -1;
    }
}

}
}
