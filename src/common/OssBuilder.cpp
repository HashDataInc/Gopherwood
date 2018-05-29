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

#include "common/OssBuilder.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Logger.h"

#include <boost/interprocess/mapped_region.hpp>
#include <fstream>

namespace Gopherwood {
namespace Internal {

OssBuilder::OssBuilder() {
    buildOssInfo();
    setObjectStorInfo();
}

shared_ptr<OssBuilder> OssBuilder::instance = NULL;

shared_ptr<OssBuilder> OssBuilder::getInstance() {
    if (!instance)
        instance = shared_ptr<OssBuilder>(new OssBuilder());
    return instance;
}

ossContext OssBuilder::buildContext() {
    ossContext ctx = NULL;

    LOG(DEBUG2, "%s\n%s\n%s\n%s\n%s\n",
        mOssInfo.object_stor_type.c_str(),
        mOssInfo.liboss_zone.c_str(),
        mOssInfo.liboss_appid.c_str(),
        mOssInfo.liboss_access_key_id.c_str(),
        mOssInfo.liboss_secret_access_key.c_str());
    ctx = ossInitContext(
            mOssInfo.object_stor_type.c_str(),
            mOssInfo.liboss_zone.c_str(),
            mOssInfo.liboss_appid.c_str(),
            mOssInfo.liboss_access_key_id.c_str(),
            mOssInfo.liboss_secret_access_key.c_str(),
            mOssInfo.liboss_write_buffer,
            mOssInfo.liboss_read_buffer);

    if (ctx == NULL) {
        THROW(GopherwoodInvalidParmException,
              "[OssBuilder] OSS context initialization failed!");
    }
    return ctx;
}

std::string OssBuilder::getBucketName() {
    return mBucket;
}

void OssBuilder::buildOssInfo() {
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

    LOG(DEBUG1, "[OssBuilder]            |"
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
            mBucket = mkey.substr(eq_c_pos + 1);
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
            LOG(DEBUG1,
                "OssBuilder: liboss write buffer size set to %ld",
                mOssInfo.qs_write_buffer);
        }
        if (mkey.find("fs.qingstor.read_buffer") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            std::stringstream sstr(mkey.substr(eq_c_pos + 1));
            sstr >> mOssInfo.qs_read_buffer;
            LOG(DEBUG1,
                "OssBuilder: liboss read buffer size set to %ld",
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
            LOG(DEBUG1,
                "OssBuilder: liboss write buffer size set to %ld",
                mOssInfo.s3_write_buffer);
        }
        if (mkey.find("fs.s3.read_buffer") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            std::stringstream sstr(mkey.substr(eq_c_pos + 1));
            sstr >> mOssInfo.s3_read_buffer;
            LOG(DEBUG1,
                "OssBuilder: liboss read buffer size set to %ld",
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
            LOG(DEBUG1,
                "OssBuilder: liboss write buffer size set to %ld",
                mOssInfo.txcos_write_buffer);
        }
        if (mkey.find("fs.txcos.read_buffer") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            std::stringstream sstr(mkey.substr(eq_c_pos + 1));
            sstr >> mOssInfo.txcos_read_buffer;
            LOG(DEBUG1,
                "OssBuilder: liboss read buffer size set to %ld",
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
            LOG(DEBUG1,
                "OssBuilder: liboss write buffer size set to %ld",
                mOssInfo.alioss_write_buffer);
        }
        if (mkey.find("fs.oss.read_buffer") != std::string::npos) {
            std::string::size_type eq_c_pos = mkey.find('=');
            std::stringstream sstr(mkey.substr(eq_c_pos + 1));
            sstr >> mOssInfo.alioss_read_buffer;
            LOG(DEBUG1,
                "OssBuilder: liboss read buffer size set to %ld",
                mOssInfo.alioss_read_buffer);
        }
        // TO DO: ks3
    }
    fin.close();
}

void OssBuilder::setObjectStorInfo() {
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

OssBuilder::~OssBuilder() {};

}
}