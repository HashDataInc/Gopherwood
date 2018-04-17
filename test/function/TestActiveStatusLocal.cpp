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

using namespace Gopherwood;
using namespace Gopherwood::Internal;


class TestActiveStatusLocal: public ::testing::Test {
public:
    TestActiveStatusLocal()
    {
        try {
            sprintf(workDir, "/data/gopherwood");

            GWContextConfig config;
            config.blockSize = 10;
            config.numBlocks = 50;
            config.numPreDefinedConcurrency = 10;
            config.severity = GW_LogSeverity::INFO;
            fs =  gwCreateContext(workDir, &config);
            buffer = (char *) malloc(100);
        } catch (...) {

        }
    }

    ~TestActiveStatusLocal() {
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

TEST_F(TestActiveStatusLocal, TestFormatContext) {
    ASSERT_NO_THROW(gwFormatContext(workDir));
}

/* test read while the file is writing by another activeStatus */
TEST_F(TestActiveStatusLocal, TestWriteReadConcurrent) {
    char input[] = "aaaaaaaaaabbbbbbbbbbcccccccccc";
    char fileName[] = "TestFormatWorkDir/TestWriteReadConcurrent";
    int64_t pos;
    int len;
    gwFile file = NULL;
    gwFile file1 = NULL;

    ASSERT_NO_THROW(file = gwOpenFile(fs, fileName, GW_CREAT|GW_RDWR));
    ASSERT_NO_THROW(len = gwWrite(fs, file, input, 30));
    EXPECT_EQ(30, len);
    ASSERT_NO_THROW(gwFlush(fs, file));

    ASSERT_NO_THROW(pos = gwSeek(fs, file, 10, SEEK_SET));
    EXPECT_EQ(10, pos);

    ASSERT_NO_THROW(len = gwRead(fs, file, buffer, 20));
    buffer[len] = '\0';
    EXPECT_EQ(20, len);
    EXPECT_STREQ(input+10, buffer);

    ASSERT_NO_THROW(file1 = gwOpenFile(fs, fileName, GW_RDONLY));

    ASSERT_NO_THROW(pos = gwSeek(fs, file1, 10, SEEK_SET));
    EXPECT_EQ(10, pos);

    ASSERT_NO_THROW(len = gwRead(fs, file1, buffer, 20));
    buffer[len] = '\0';
    EXPECT_EQ(20, len);
    EXPECT_STREQ(input+10, buffer);

    ASSERT_NO_THROW(gwCloseFile(fs, file1));
    ASSERT_NO_THROW(gwCloseFile(fs, file));
    ASSERT_NO_THROW(gwDeleteFile(fs, fileName));
}

/* Test Seek Exceed Eof */
TEST_F(TestActiveStatusLocal, TestSeekExceedEof) {
    char input[] = "aaaaaaaaaabbbbbbbbbb";
    char fileName[] = "TestFormatWorkDir/TestSeekExceedEof";
    int64_t pos;
    int len;
    gwFile file = NULL;

    ASSERT_NO_THROW(file = gwOpenFile(fs, fileName, GW_CREAT|GW_RDWR));
    pos = gwSeek(fs, file, 10, SEEK_SET);
    EXPECT_EQ(10, pos);
    ASSERT_NO_THROW(len = gwWrite(fs, file, input, 20));
    EXPECT_EQ(20, len);
    ASSERT_NO_THROW(pos = gwSeek(fs, file, 0, SEEK_CUR));
    EXPECT_EQ(30, pos);

    ASSERT_NO_THROW(pos = gwSeek(fs, file, 10, SEEK_SET));
    EXPECT_EQ(10, pos);

    ASSERT_NO_THROW(len = gwRead(fs, file, buffer, 20));
    EXPECT_EQ(20, len);
    buffer[len] = '\0';
    EXPECT_STREQ(input, buffer);

    ASSERT_NO_THROW(gwCloseFile(fs, file));
    ASSERT_NO_THROW(gwDeleteFile(fs, fileName));
}

TEST_F(TestActiveStatusLocal, TestWriteExceedQuota) {
    char input[] = "0123456789";
    char expect_output[] = "024680246802468";
    char fileName[] = "TestFormatWorkDir/TestWriteExceedQuota";
    gwFile file = NULL;
    gwFile file1 = NULL;
    int64_t pos;
    int len;

    ASSERT_NO_THROW(file = gwOpenFile(fs, fileName, GW_CREAT|GW_RDWR));
    for (int i=0; i<5; i++) {
        ASSERT_NO_THROW(len = gwWrite(fs, file, input, 10));
        EXPECT_EQ(10, len);
    }
    ASSERT_NO_THROW(gwCloseFile(fs, file));

    ASSERT_NO_THROW(file1 = gwOpenFile(fs, fileName, GW_RDONLY));
    int ind = 0;
    for (int t_pos = 0; t_pos <30; t_pos+=2){
        ASSERT_NO_THROW(pos = gwSeek(fs, file1, t_pos, SEEK_SET));
        EXPECT_EQ(t_pos, pos);
        ASSERT_NO_THROW(len = gwRead(fs, file1, buffer+ind, 1));
        EXPECT_EQ(1, len);
        ind+=1;
    }
    buffer[ind] = '\0';
    EXPECT_STREQ(expect_output, buffer);

    ASSERT_NO_THROW(gwCloseFile(fs, file1));
    ASSERT_NO_THROW(gwDeleteFile(fs, fileName));
}