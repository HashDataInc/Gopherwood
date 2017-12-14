///********************************************************************
// * 2016 -
// * open source under Apache License Version 2.0
// ********************************************************************/
///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//#include "gtest/gtest.h"
//#include "core/gopherwood.h"
//#include "common/Configuration.h"
//#include "core/FileSystem.h"
//#include "core/FileSystemInter.h"
//#include "core/FileSystemImpl.h"
//#include "core/FileStatus.h"
//
//#include "../../src/core/FileStatus.h"
//#include "../../src/core/FileSystem.h"
//
//
//#include <bits/shared_ptr.h>
//
//#ifndef _TEST_GOPHERWOOD_PREFIX_
//#define _TEST_GOPHERWOOD_PREFIX_ "/"
//#endif
//
//using  namespace Gopherwood;
//using namespace Gopherwood::Internal;
//class TestFileSystem : public ::testing::Test {
//public:
//
//    TestFileSystem(){
//        printf("Default Constructor of TestFileSystem");
//    }
//
//    ~TestFileSystem() {
//        printf("Default DeConstructor of TestFileSystem");
//    }
//
//protected:
//    std::shared_ptr<FileSystemInter> filesystem;
//};
//
//TEST_F(TestFileSystem, easyTest) {
//    printf("test easyTest");
//}
//
//
//TEST_F(TestFileSystem, checkFileExist) {
//    char *fileName = "file-createContext";
//    filesystem =  std::shared_ptr<FileSystemInter>(new FileSystemImpl(fileName));
//    ASSERT_FALSE(filesystem->checkFileExist(fileName));
//}
//
//
//TEST_F(TestFileSystem, createFile) {
//    char *fileName = "file-createFile";
//    filesystem =  std::shared_ptr<FileSystemInter>(new FileSystemImpl(fileName));
//    filesystem->createFile(fileName);
//    std::shared_ptr<FileStatus> status = filesystem->getFileStatus(fileName);
//    ASSERT_EQ(status->getFileName(),fileName);
//    ASSERT_EQ(status->getBlockIdVector().size(),0);
//    ASSERT_EQ(status->getLastBucket(),0);
//    ASSERT_EQ(status->getEndOffsetOfBucket(),0);
//    std::cout<<"status->getBlockIdVector().size() = "<<status->getBlockIdVector().size()<<std::endl;
//    std::cout<<"status->status->getLastBucket() = "<<status->getLastBucket()<<std::endl;
//}
//
//TEST_F(TestFileSystem, acquireNewBlock) {
//    char *fileName = "file-createFile";
//    filesystem =  std::shared_ptr<FileSystemInter>(new FileSystemImpl(fileName));
//    filesystem->createFile(fileName);
//    filesystem->acquireNewBlock(fileName);
//    std::shared_ptr<FileStatus> status = filesystem->getFileStatus(fileName);
//    ASSERT_EQ(status->getFileName(),fileName);
//    ASSERT_EQ(status->getBlockIdVector().size(),1);
//
//    ASSERT_EQ(status->getEndOffsetOfBucket(),0);
//
//    std::cout<<"status->status->getLastBucket() = "<<status->getLastBucket()<<std::endl;
//}
//
//int main(int argc, char **argv) {
//    ::testing::InitGoogleTest(&argc, argv);
//#ifdef DATA_DIR
//    if (0 != chdir(DATA_DIR)) {
//        abort();
//    }
//#endif
//    return RUN_ALL_TESTS();
//}
