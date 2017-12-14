////
//// Created by houliang on 12/13/17.
////
//
//#ifndef _TEST_GOPHERWOOD_PREFIX_
//#define _TEST_GOPHERWOOD_PREFIX_ "./"
//#endif
//
//
//#include "gtest/gtest.h"
//#include "core/gopherwood.h"
//#include "core/FileStatus.h"
//
//
//#include "../../src/core/FileSystemInter.h"
//#include "../../src/core/LogFormat.h"
//#include "../../src/util/Coding.h"
//
//
//using namespace Gopherwood;
//using namespace Gopherwood::Internal;
//
//class TestLogFormat : public ::testing::Test {
//public:
//
//    TestLogFormat() {
//        printf("Default Constructor of TestLogFormat");
//    }
//
//    ~TestLogFormat() {
//        printf("Default DeConstructor of TestLogFormat");
//    }
//
//protected:
//};
//
//TEST(TestLogFormat, acquireNewBlock) {
//
//    vector<int32_t> blockIdVector = {1, 3, 8};
//    char *fileName = "filename-acquireNewBlock";
//
//    LogFormat *logFormat = new LogFormat();
//
//    string resStr = logFormat->serializeLog(blockIdVector, LogFormat::RecordType::acquireNewBlock);
//    char *res = (char *) resStr.data();
//    int offset = 0;
//    cout << "resStr size=" << resStr.size() << endl;
//
//    int totalSize = DecodeFixed32(res + offset);
//    offset+=4;
//    cout << "totalSize=" << totalSize << endl;
//    ASSERT_EQ(totalSize, 4+4*3);
//
//
//    char type = *(res + offset);
//    offset+=1;
//
//    cout << "before=" << (LogFormat::RecordType::acquireNewBlock & 0x0F) << endl;
//    cout << "type=" << type << endl;
//    char before = LogFormat::RecordType::acquireNewBlock & 0x0F;
//    ASSERT_EQ(type, before);
//
//
//
//    int numOfBlocks = DecodeFixed32(res + offset);
//    offset += 4;
//    cout << "numOfBlocks=" << numOfBlocks << endl;
//    ASSERT_EQ(numOfBlocks, blockIdVector.size());
//
//
//    vector<int32_t> tmpVector;
//    for (int i = 0; i < numOfBlocks; i++) {
//        int blockID = DecodeFixed32(res + offset);
//        tmpVector.push_back(blockID);
//        offset += 4;
//    }
//
//    ASSERT_EQ(blockIdVector, tmpVector);
//
//    int pid = DecodeFixed32(res + offset);
//    cout << "TestLogFormat pid=" << pid << endl;
//
//
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