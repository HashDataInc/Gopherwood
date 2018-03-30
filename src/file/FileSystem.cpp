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
    LOG(INFO, "[FileSystem]            |"
            "Format SharedMemory %s", Configuration::SHARED_MEMORY_NAME.c_str());
}

FileSystem::FileSystem(const char *workDir) :
        workDir(workDir) {
    /* open local space file */
    std::stringstream ss;
    ss << workDir << '/' << Configuration::LOCAL_SPACE_FILE;
    std::string filePath = ss.str();
    int flags = O_CREAT | O_RDWR;
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
    status = mActiveStatusContext->initFileActiveStatus(fileId, isWrite, mLocalSpaceFile);

    LOG(INFO, "[FileSystem]            |"
            "Creating file %s", fileId.toString().c_str());
    std::string name(fileName);
    return new File(fileId, name, flags, mLocalSpaceFile, status);
}

File *FileSystem::OpenFile(const char *fileName, int flags, bool isWrite) {
    FileId fileId;
    shared_ptr<ActiveStatus> status;

    fileId = makeFileId(std::string(fileName));
    status = mActiveStatusContext->openFileActiveStatus(fileId, isWrite, mLocalSpaceFile);

    LOG(INFO, "[FileSystem]            |"
            "Opening file %s", fileId.toString().c_str());
    std::string name(fileName);
    return new File(fileId, name, flags, mLocalSpaceFile, status);
}

void FileSystem::CloseFile(File &file) {
    file.close();
}

void FileSystem::DeleteFile(const char *fileName) {
    FileId delFileId = makeFileId(std::string(fileName));
    shared_ptr<ActiveStatus> status;

    /* open file with delete type */
    status = mActiveStatusContext->deleteFileActiveStatus(delFileId, mLocalSpaceFile);

    /* call activeStatus destroy */
    status->close();
    status.reset();
}

void FileSystem::buildOssInfo() {
    std::string conf_file;
    std::string properties_path = "/conf/alluxio-site.properties";

    char *gopherwood_conf = getenv("GOPHERWOOD_CONF");
    char *alluxio_home = getenv("ALLUXIO_HOME");

    if (gopherwood_conf) {
        conf_file = std::string(gopherwood_conf);
    } else if (alluxio_home) {
        conf_file = std::string(alluxio_home) + properties_path;
    } else {
        DIR *dir = opendir("/opt/alluxio");
        if (dir) {
            closedir(dir);
            conf_file = "/opt/alluxio" + properties_path;
        } else {
            THROW(GopherwoodException,
                  "Gopherwood conf file not found!");
            return;
        }
    }

    LOG(INFO, "[FileSystem]            |"
            "Oss configuration file is %s", conf_file.c_str());

    std::ifstream fin(conf_file.c_str());
    if (!fin) {
        THROW(GopherwoodException,
              "Can not open configure file alluxio-site.properties");
        return;
    }

    std::string mkey;
    while (!fin.eof()) {
        getline(fin, mkey);

        if (mkey[0] == '#')
            continue;

        /**
         *  liboss settings
         *    All OSSs' configuration will be loaded together,
         *    and set in use according to OSStype parsed
         *    from ufsPath by setObjectStorInfo().
         */
        //oss_type
        if (mkey.find("oss.type") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            mOssInfo.object_stor_type = mkey.substr(eq_c_pos + 1);
        }
        if (mkey.find("oss.bucket") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            OSS_BUCKET = mkey.substr(eq_c_pos + 1);
        }

        //qingstor
        if (mkey.find("fs.qingstor.accessKeyId") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            mOssInfo.qs_access_key_id = mkey.substr(eq_c_pos + 1);
        }
        if (mkey.find("fs.qingstor.secretAccessKey") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            mOssInfo.qs_secret_access_key = mkey.substr(eq_c_pos + 1);
        }
        if (mkey.find("fs.qingstor.zone") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            mOssInfo.qs_zone = mkey.substr(eq_c_pos + 1);
        }
        if (mkey.find("fs.qingstor.write_buffer") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            std::stringstream sstr(mkey.substr(eq_c_pos + 1));
            sstr >> mOssInfo.qs_write_buffer;
            LOG(INFO,
                "FileSystemContext: liboss write buffer size set to %ld",
                mOssInfo.qs_write_buffer);
        }
        if (mkey.find("fs.qingstor.read_buffer") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            std::stringstream sstr(mkey.substr(eq_c_pos + 1));
            sstr >> mOssInfo.qs_read_buffer;
            LOG(INFO,
                "FileSystemContext: liboss read buffer size set to %ld",
                mOssInfo.qs_read_buffer);
        }

        //s3
        if (mkey.find("aws.accessKeyId") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            mOssInfo.s3_access_key_id = mkey.substr(eq_c_pos + 1);
        }
        if (mkey.find("aws.secretKey") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            mOssInfo.s3_secret_access_key = mkey.substr(eq_c_pos + 1);
        }
        if (mkey.find("alluxio.underfs.s3.endpoint") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            mOssInfo.s3_zone = mkey.substr(eq_c_pos + 1);
        }
        if (mkey.find("fs.s3.write_buffer") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            std::stringstream sstr(mkey.substr(eq_c_pos + 1));
            sstr >> mOssInfo.s3_write_buffer;
            LOG(INFO,
                "FileSystemContext: liboss write buffer size set to %ld",
                mOssInfo.s3_write_buffer);
        }
        if (mkey.find("fs.s3.read_buffer") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            std::stringstream sstr(mkey.substr(eq_c_pos + 1));
            sstr >> mOssInfo.s3_read_buffer;
            LOG(INFO,
                "FileSystemContext: liboss read buffer size set to %ld",
                mOssInfo.s3_read_buffer);
        }

        //cos
        if (mkey.find("fs.txcos.accessKeyId") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            mOssInfo.txcos_access_key_id = mkey.substr(eq_c_pos + 1);
        }
        if (mkey.find("fs.txcos.secretAccessKey") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            mOssInfo.txcos_secret_access_key = mkey.substr(eq_c_pos + 1);
        }
        if (mkey.find("fs.txcos.appid") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            mOssInfo.txcos_appid = mkey.substr(eq_c_pos + 1);
        }
        if (mkey.find("fs.txcos.zone") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            mOssInfo.txcos_zone = mkey.substr(eq_c_pos + 1);
        }
        if (mkey.find("fs.txcos.write_buffer") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            std::stringstream sstr(mkey.substr(eq_c_pos + 1));
            sstr >> mOssInfo.txcos_write_buffer;
            LOG(INFO,
                "FileSystemContext: liboss write buffer size set to %ld",
                mOssInfo.txcos_write_buffer);
        }
        if (mkey.find("fs.txcos.read_buffer") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            std::stringstream sstr(mkey.substr(eq_c_pos + 1));
            sstr >> mOssInfo.txcos_read_buffer;
            LOG(INFO,
                "FileSystemContext: liboss read buffer size set to %ld",
                mOssInfo.txcos_read_buffer);
        }

        //ali OSS
        if (mkey.find("fs.oss.accessKeyId") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            mOssInfo.alioss_access_key_id = mkey.substr(eq_c_pos + 1);
        }
        if (mkey.find("fs.oss.accessKeySecret") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            mOssInfo.alioss_secret_access_key = mkey.substr(eq_c_pos + 1);
        }
        if (mkey.find("fs.oss.zone") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            mOssInfo.alioss_zone = mkey.substr(eq_c_pos + 1);
        }
        if (mkey.find("fs.oss.write_buffer") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            std::stringstream sstr(mkey.substr(eq_c_pos + 1));
            sstr >> mOssInfo.alioss_write_buffer;
            LOG(INFO,
                "FileSystemContext: liboss write buffer size set to %ld",
                mOssInfo.alioss_write_buffer);
        }
        if (mkey.find("fs.oss.read_buffer") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            std::stringstream sstr(mkey.substr(eq_c_pos + 1));
            sstr >> mOssInfo.alioss_read_buffer;
            LOG(INFO,
                "FileSystemContext: liboss read buffer size set to %ld",
                mOssInfo.alioss_read_buffer);
        }
        // TO DO: ks3
    }
    fin.close();
}

void FileSystem::setObjectStorInfo() {
    if (strcmp(mOssInfo.object_stor_type.c_str(), "QS") == 0) {
        mOssInfo.object_stor_type = "QS";
        mOssInfo.liboss_access_key_id = mOssInfo.qs_access_key_id;
        mOssInfo.liboss_secret_access_key = mOssInfo.qs_secret_access_key;
        mOssInfo.liboss_zone = mOssInfo.qs_zone;
        mOssInfo.liboss_write_buffer = mOssInfo.qs_write_buffer;
        mOssInfo.liboss_read_buffer = mOssInfo.qs_read_buffer;
    } else if (strcmp(mOssInfo.object_stor_type.c_str(), "S3") == 0) {
        mOssInfo.object_stor_type = "S3B";
        mOssInfo.liboss_access_key_id = mOssInfo.s3_access_key_id;
        mOssInfo.liboss_secret_access_key = mOssInfo.s3_secret_access_key;
        mOssInfo.liboss_zone = mOssInfo.s3_zone;
        mOssInfo.liboss_write_buffer = mOssInfo.s3_write_buffer;
        mOssInfo.liboss_read_buffer = mOssInfo.s3_read_buffer;
    } else if (strcmp(mOssInfo.object_stor_type.c_str(), "TXCOS") == 0) {
        mOssInfo.object_stor_type = "COS";
        mOssInfo.liboss_access_key_id = mOssInfo.txcos_access_key_id;
        mOssInfo.liboss_secret_access_key = mOssInfo.txcos_secret_access_key;
        mOssInfo.liboss_appid = mOssInfo.txcos_appid;
        mOssInfo.liboss_zone = mOssInfo.txcos_zone;
        mOssInfo.liboss_write_buffer = mOssInfo.txcos_write_buffer;
        mOssInfo.liboss_read_buffer = mOssInfo.txcos_read_buffer;
    } else if (strcmp(mOssInfo.object_stor_type.c_str(), "OSS") == 0) {
        mOssInfo.object_stor_type = "ALI";
        mOssInfo.liboss_access_key_id = mOssInfo.alioss_access_key_id;
        mOssInfo.liboss_secret_access_key = mOssInfo.alioss_secret_access_key;
        mOssInfo.liboss_zone = mOssInfo.alioss_zone;
        mOssInfo.liboss_write_buffer = mOssInfo.alioss_write_buffer;
        mOssInfo.liboss_read_buffer = mOssInfo.alioss_read_buffer;
    }
    // TO DO : other OSS
}

void FileSystem::initOssContext() {
    buildOssInfo();
    setObjectStorInfo();

    if (OSS_CONTEXT == NULL) {
        printf("%s\n%s\n%s\n%s\n%s\n",
               mOssInfo.object_stor_type.c_str(),
               mOssInfo.liboss_zone.c_str(),
               mOssInfo.liboss_appid.c_str(),
               mOssInfo.liboss_access_key_id.c_str(),
               mOssInfo.liboss_secret_access_key.c_str());
        OSS_CONTEXT = ossInitContext(
                mOssInfo.object_stor_type.c_str(),
                mOssInfo.liboss_zone.c_str(),
                mOssInfo.liboss_appid.c_str(),
                mOssInfo.liboss_access_key_id.c_str(),
                mOssInfo.liboss_secret_access_key.c_str(),
                mOssInfo.liboss_write_buffer,
                mOssInfo.liboss_read_buffer);
    }
    if (OSS_CONTEXT == NULL) {
        THROW(GopherwoodInvalidParmException,
              "[FileSystem] OSS context initialization failed!");
    }
}

FileSystem::~FileSystem() {

}

}
}
