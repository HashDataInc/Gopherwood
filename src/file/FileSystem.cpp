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
#include "FileSystem.h"
#include "common/Configuration.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Hash.h"

namespace Gopherwood {
namespace Internal {

void FileSystem::Format(const char *workDir){
    std::stringstream ss;
    ss << "exec rm -r " << workDir << "/*";
    system(ss.str().c_str());
    shared_memory_object::remove(Configuration::SHARED_MEMORY_NAME.c_str());
    LOG(INFO, "[FileSystem] %s", Configuration::SHARED_MEMORY_NAME.c_str());
}

FileSystem::FileSystem(const char *workDir) :
        workDir(workDir) {
    /* open local space file */
    std::stringstream ss;
    ss << workDir << '/' << Configuration::LOCAL_SPACE_FILE;
    std::string filePath = ss.str();
    int flags = O_CREAT|O_RDWR;
    mLocalSpaceFile = open(filePath.c_str(), flags, 0644);

    /* create lock file */
    ss.str("");
    ss << workDir << "/SmLock";
    filePath = ss.str();
    int32_t lockFile = open(filePath.c_str(), flags, 0644);

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

FileId FileSystem::makeFileId(const std::string filePath)
{
    FileId id;

    /* hash the path to size_t */
    id.hashcode = StringHasher(filePath);

    /* if the hashcode collates with other files,
     * assign an identical collision id
     * TODO: Not implemented, need to check existing files */
    id.collisionId = 0;

    return id;
}

void FileSystem::initOssContext() {
    /* TODO: Dynamically inject these parms */
    std::string object_stor_type = "QS";
    std::string liboss_zone = "pek3a";
    std::string liboss_appid = "";
    std::string liboss_access_key_id = "UITUBKOLLATHRMDBIMYC";
    std::string liboss_secret_access_key = "PHxFQ0qA9hWRsvYM0OKPCW6VZxW6PYb9AZ2zZMXF";
    int64_t liboss_write_buffer = 8 << 20;
    int64_t liboss_read_buffer = 32 << 20;

    mOssContext = ossInitContext(
            object_stor_type.c_str(),
            liboss_zone.c_str(),
            liboss_appid.c_str(),
            liboss_access_key_id.c_str(),
            liboss_secret_access_key.c_str(),
            liboss_write_buffer,
            liboss_read_buffer);

    if (mOssContext == NULL){
        THROW(GopherwoodInvalidParmException,
              "[FileSystem] OSS context initialization failed!");
    }
}

File* FileSystem::CreateFile(const char *fileName, int flags, bool isWrite)
{
    FileId fileId;
    shared_ptr<ActiveStatus> status;

    fileId = makeFileId(std::string(fileName));
    status = mActiveStatusContext->initFileActiveStatus(fileId, isWrite);

    LOG(INFO, "[FileSystem] Creating file %s", fileId.toString().c_str());
    std::string name(fileName);
    return new File(fileId, name, flags, mLocalSpaceFile, status, mOssContext);
}

File* FileSystem::OpenFile(const char *fileName, int flags, bool isWrite)
{
    FileId fileId;
    shared_ptr<ActiveStatus> status;

    fileId = makeFileId(std::string(fileName));
    status = mActiveStatusContext->openFileActiveStatus(fileId, isWrite);

    LOG(INFO, "[FileSystem] Opening file %s", fileId.toString().c_str());
    std::string name(fileName);
    return new File(fileId, name, flags, mLocalSpaceFile, status, mOssContext);
}

void FileSystem::removeActiveFileStatus(FileId fileId){
    mActiveStatusContext->removeActiveStatus(fileId);
}

FileSystem::~FileSystem() {

}

}
}
