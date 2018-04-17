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

class TestActiveStatusRemote: public ::testing::Test {
public:
    TestActiveStatusRemote()
    {
        try {
            sprintf(workDir, "/data/gopherwood");

            GWContextConfig config;
            config.blockSize = 10;
            config.numBlocks = 50;
            config.numPreDefinedConcurrency = 10;
            config.severity = GW_LogSeverity::INFO;

            fs =  gwCreateContext(workDir, &config);
        } catch (...) {

        }
    }

    ~TestActiveStatusRemote() {
        try {
            gwDestroyContext(fs);
        } catch (...) {
        }
    }

protected:
    char workDir[40];
    gopherwoodFS fs;

};

TEST_F(TestActiveStatusRemote, TestFormatContext) {
    ASSERT_NO_THROW(gwFormatContext(workDir));
}
