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

#include <fstream>
#include <iostream>
#include <fcntl.h>
#include <sstream>
#include <cstring>

#ifndef DATA_DIR
#define DATA_DIR ""
#endif

using namespace Gopherwood;
using namespace Gopherwood::Internal;

class TestIctCases: public ::testing::Test {
public:
    TestIctCases()
    {
        try {
            sprintf(workDir, "/data/gopherwood");

            GWContextConfig config;
            config.blockSize = 1024;
            config.numBlocks = 10;
            config.numPreDefinedConcurrency = 2;
            config.severity = LOGSEV_DEBUG1;
            fs =  gwCreateContext(workDir, &config);

            buffer = (char *) malloc(100);
        } catch (...) {

        }
    }

    void writeUtil(char *fileName, char *buf, int size) {
        std::ofstream ostrm(fileName, std::ios::out | std::ios::app);
        ostrm.write(buf, size);
    }

    ~TestIctCases() {
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

TEST_F(TestIctCases, TestFormatContext) {
    ASSERT_NO_THROW(gwFormatContext(workDir));
}

/* Sequentially write a file and read it back, compare the MD5 checksum.
 * The test have used up it's local quota and need to evict block to OSS.*/
TEST_F(TestIctCases, TestPreload) {
    char fileName[] = "TestIctCases/TestPreload";
    gwFile file = gwOpenFile(fs, fileName, GW_CREAT | GW_RDWR);

    int SIZE = 128;

    //3. construct the file name
    std::stringstream ss;
    ss << DATA_DIR"UserData";
    std::string filePath = ss.str();

    //4. read data from file
    std::ifstream infile;
    printf("===12312312-====%s===\n", filePath.c_str());
    infile.open(filePath);


    int totalWriteLength = 0;
    char *buf = new char[SIZE];
    infile.read(buf, SIZE);
    int readLengthIn = infile.gcount();
    while (readLengthIn > 0) {
        totalWriteLength += readLengthIn;
        std::cout << "totalWriteLength=" << totalWriteLength << ",readLength="
                  << readLengthIn << std::endl;
        std::cout << "buf=" << buf << std::endl;
        //5. write data to the gopherwood
        std::cout << "readLengthIn=" << readLengthIn << std::endl;
        gwWrite(fs, file, buf, readLengthIn);

        std::cout << "come in =" << readLengthIn << std::endl;

        buf = new char[SIZE];
        infile.read(buf, SIZE);
        readLengthIn = infile.gcount();
    }

    gwCloseFile(fs, file);

    std::cout << "*******END OF WRITE*****, totalWriteLength=" << totalWriteLength << std::endl;


    file = gwOpenFile(fs, fileName, GW_RDONLY);

    //3. construct the file name
    std::stringstream ss1;
    ss1 << DATA_DIR"UserData-read";
    std::string fileNameForWrite = ss1.str();
    printf("===12312312-====%s===\n", fileNameForWrite.c_str());

    char *readBuf = new char[SIZE];
    int readLength = gwRead(fs, file, readBuf, SIZE);

    int totalLength = 0;
    while (readLength > 0) {
        //4. write data to file to check
        totalLength += readLength;
        writeUtil((char *) fileNameForWrite.c_str(), readBuf, readLength);

        readBuf = new char[SIZE];
        readLength = gwRead(fs, file, readBuf, SIZE);
        std::cout << "**************** readLength =  *******************" << readLength << std::endl;
    }


    gwCloseFile(fs, file);

    std::cout << "*******END OF READ*****, totalLength=" << totalLength << std::endl;
}
