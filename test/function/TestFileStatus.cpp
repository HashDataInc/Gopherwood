////
//// Created by houliang on 12/5/17.
////
//#ifndef _TEST_GOPHERWOOD_PREFIX_
//#define _TEST_GOPHERWOOD_PREFIX_ "./"
//#endif
//
//#include "gtest/gtest.h"
//#include "core/gopherwood.h"
//#include "core/FileStatus.h"
//
//
//#include "../../src/core/FileSystemInter.h"
//
//
//using namespace Gopherwood;
//using namespace Gopherwood::Internal;
//
//class TestFileStatus : public ::testing::Test {
//public:
//
//    TestFileStatus() {
//        printf("Default Constructor of TestFileStatus");
//    }
//
//    ~TestFileStatus() {
//        printf("Default DeConstructor of TestFileStatus");
//    }
//
//protected:
//};
//
//TEST_F(TestFileStatus, serializeAndDeserializeFileStatus) {
//
//    vector<int32_t> blockIdVector = {1, 5, 6};
//    char *fileName = "filename-serializeFileStatus";
//    int lastBucket = 45;
//    int64_t endOffsetOfBucket = 54603;
//
//    FileStatus *beforeStatus = new FileStatus();
//    beforeStatus->setEndOffsetOfBucket(endOffsetOfBucket);
//    beforeStatus->setFileName(fileName);
//    beforeStatus->setBlockIdVector(blockIdVector);
//    beforeStatus->setLastBucket(lastBucket);
//
//    char* res = beforeStatus->serializeFileStatus();
//
//    FileStatus *afterStatus = beforeStatus->deSerializeFileStatus(res);
//    cout << "endOffsetOfBucket = " << afterStatus->getEndOffsetOfBucket() << endl;
//    cout << "lastBucket = " << afterStatus->getLastBucket() << endl;
//    cout << "fileName = " << afterStatus->getFileName() << endl;
//    cout << "res =" << res << endl;
//    for (int i = 0; i < afterStatus->getBlockIdVector().size(); i++) {
//        cout << "afterStatus[" << i << "]" << afterStatus->getBlockIdVector()[i] << endl;
//    }
//
//    ASSERT_EQ(afterStatus->getFileName(), fileName);
//    ASSERT_EQ(afterStatus->getEndOffsetOfBucket(), endOffsetOfBucket);
//    ASSERT_EQ(afterStatus->getLastBucket(), lastBucket);
//    ASSERT_EQ(afterStatus->getBlockIdVector(), blockIdVector);
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