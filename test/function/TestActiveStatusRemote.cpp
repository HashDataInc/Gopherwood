/********************************************************************
 * 2016 -
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
#include "client/gopherwood.h"
#include "common/DateTime.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "gtest/gtest.h"
#include <openssl/md5.h>

#ifndef DATA_DIR
#define DATA_DIR ""
#endif

using namespace Gopherwood;
using namespace Gopherwood::Internal;

class TestActiveStatusRemote: public ::testing::Test {
public:
    TestActiveStatusRemote()
    {
        try {
            sprintf(workDir, "/data/gopherwood");

            GWContextConfig config;
            config.blockSize = 40;
            config.numBlocks = 50;
            config.numPreDefinedConcurrency = 10;
            config.severity = GW_LogSeverity::INFO;
            fs =  gwCreateContext(workDir, &config);

            buffer = (char *) malloc(100);
        } catch (...) {

        }
    }

    ~TestActiveStatusRemote() {
        try {
            gwDestroyContext(fs);
            free(buffer);
        } catch (...) {
        }
    }

protected:
    char workDir[40];
    gopherwoodFS fs;
    char *buffer;
};

TEST_F(TestActiveStatusRemote, TestFormatContext) {
    ASSERT_NO_THROW(gwFormatContext(workDir));
}

/* the test have used up it's local quota and need to evict block to OSS */
TEST_F(TestActiveStatusRemote, TestWriteFileExceedLocalQuota) {
    char fileName[] = "TestActiveStatusRemote/TestWriteFileExceedLocalQuota";
    unsigned char md5in[MD5_DIGEST_LENGTH];
    unsigned char md5out[MD5_DIGEST_LENGTH];
    MD5_CTX mdContext;

    gwFile file = NULL;
    int len;

    /* write to Gopherwood file */
    MD5_Init(&mdContext);
    int readFd = open(DATA_DIR"testfile1.md", O_RDWR);
    ASSERT_NO_THROW(file = gwOpenFile(fs, fileName, GW_CREAT|GW_RDWR));
    while(true) {
        len = read(readFd, buffer, 100);
        if (len > 0) {
            MD5_Update (&mdContext, buffer, len);
            ASSERT_NO_THROW(len = gwWrite(fs, file, buffer, len));
        }
        else {
            break;
        }
    }
    MD5_Final(md5in,&mdContext);
    //for(int i = 0; i < MD5_DIGEST_LENGTH; i++) printf("%02x", md5in[i]);
    close(readFd);

    /* read from Gopherwood file */
    MD5_Init(&mdContext);
    ASSERT_NO_THROW(gwSeek(fs, file, 0, SEEK_SET));
    int writeFd = open(DATA_DIR"testfile1_out.md", O_RDWR|O_CREAT);
    while(true)
    {
        ASSERT_NO_THROW(len = gwRead(fs, file, buffer, 100));
        if (len > 0) {
            MD5_Update(&mdContext, buffer, len);
            write(writeFd, buffer, len);
        }
        else
            break;
    }
    MD5_Final(md5out, &mdContext);
    //for(int i = 0; i < MD5_DIGEST_LENGTH; i++) printf("%02x", md5in[i]);
    close(writeFd);

    for(int i = 0; i < MD5_DIGEST_LENGTH; i++) {
        EXPECT_EQ(md5in[i], md5out[i]);
    }

    ASSERT_NO_THROW(gwCloseFile(fs, file));
    ASSERT_NO_THROW(gwDeleteFile(fs, fileName));
}