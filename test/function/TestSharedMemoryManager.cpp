///**
// * User: neuyilan@163.com
// * Date: 12/14/17
// * Time: 11:21 AM
// */
//
//
//#ifndef _TEST_GOPHERWOOD_PREFIX_TestSharedMemoryManager_
//#define _TEST_GOPHERWOOD_PREFIX_TestSharedMemoryManager_ "./"
//
//
//#include "gtest/gtest.h"
//#include "../../src/core/FileSystemInter.h"
//#include "../../src/core/FileSystem.h"
//#include "../../src/core/OutputStreamImpl.h"
//#include "../../src/core/InputStreamImpl.h"
//
//
//using namespace Gopherwood;
//using namespace Gopherwood::Internal;
//
//class TestSharedMemoryManager : public ::testing::Test {
//public:
//
//    TestSharedMemoryManager() {
//        printf("Default Constructor of TestSharedMemoryManager");
//    }
//
//    ~TestSharedMemoryManager() {
//        printf("Default DeConstructor of TestSharedMemoryManager");
//    }
//
//protected:
//    std::shared_ptr<FileSystemInter> filesystem;
//    std::shared_ptr<OutputStreamInter> osiImpl;
//    std::shared_ptr<InputStreamInter> isImpl;
//};
//
//TEST_F(TestSharedMemoryManager, acquireNewBlock) {
//    char *fileName = "TestSharedMemoryManager-acquireNewBlock";
//    int flag = O_RDWR;
//    FileSystem *fs = NULL;
//    fs = new FileSystem(fileName);
//    //1. create context,
//    filesystem = fs->impl->filesystem;
//
//    //2. create file
//    filesystem->createFile(fileName);
//
//    //3. create output stream
//    OutputStreamImpl *tmpImpl = new OutputStreamImpl(filesystem, fileName, flag);
//    std::shared_ptr<OutputStreamInter> tmposiImpl(tmpImpl);
//    osiImpl = tmposiImpl;
//
//    //4. write data
//    char buf[1024 *
//             1024] = "hello gopherwoodaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccc";
//    for (int i = 0; i < 10; i++) {
//        osiImpl->write(buf, strlen(buf));
//    }
//
//    //5. close file
//    filesystem->closeFile(fileName);
//
//    //7. read the close file status
//    filesystem->readCloseFileStatus(fileName);
//
//}
//
//TEST_F(TestSharedMemoryManager, evictBlock) {
////   in this test, the NUMBER_OF_BLOCKS is set to 6;
//
//    char *fileName = "TestSharedMemoryManager-evictBlock";
//    int flag = O_RDWR;
//    FileSystem *fs = NULL;
//    fs = new FileSystem(fileName);
//    //1. create context,
//    filesystem = fs->impl->filesystem;
//
//    //2. create file
//    filesystem->createFile(fileName);
//
//    //3. create output stream
//    OutputStreamImpl *tmpImpl = new OutputStreamImpl(filesystem, fileName, flag);
//    std::shared_ptr<OutputStreamInter> tmposiImpl(tmpImpl);
//    osiImpl = tmposiImpl;
//
//    //4. write data
//
//    char buf[1024 *
//             1024] = "evictBlock,evictBlock, evictBlock,evictBlock,evictBlock, evictBlock,evictBlock,evictBlock,"
//            " evictBlock,evictBlock,evictBlock, evictBlock,evictBlock,evictBlock, evictBlock,evictBlock,evictBlock, "
//            "evictBlock,evictBlock,evictBlock, evictBlock,";
//
//    for (int i = 0; i < 10; i++) {
//        osiImpl->write(buf, strlen(buf));
//    }
//
//    cout << endl << "******************************************************" << endl;
//
//    char buf2[1024 *
//              1024] = "new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,"
//            "new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,"
//            "new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,"
//            "new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,new,";
//
//    for (int i = 0; i < 6; i++) {
//        osiImpl->write(buf2, strlen(buf2));
//    }
//
////    char buf3[1024 *
////              1024] = "buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,"
////            "buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,"
////            "buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,"
////            "buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,buf3,";
////
////    for (int i = 0; i < 6; i++) {
////        osiImpl->write(buf3, strlen(buf3));
////    }
//
//
//    //6. close file
//    filesystem->closeFile(fileName);
//
//    //7. read the close file status
//    filesystem->readCloseFileStatus(fileName);
//}
//
//
//TEST_F(TestSharedMemoryManager, rebuildFileStatusFromLog) {
//    std::string fileNameArr[2] = {"TestSharedMemoryManager-acquireNewBlock", "TestSharedMemoryManager-evictBlock"};
//    int length = (sizeof(fileNameArr) / sizeof(fileNameArr[0]));
//    for (int i = 0; i < length; i++) {
//        char *fileName = (char *) fileNameArr[i].data();
//        cout << "fileName= " << fileName << endl;
//        cout << "fileNameArr[i]= " << fileNameArr[i] << endl;
//        int flag = O_RDWR;
//        FileSystem *fs = NULL;
//        fs = new FileSystem(fileName);
//        //1. create context,
//        filesystem = fs->impl->filesystem;
//        cout << "context is created" << endl;
//
//        //2. see the final log status is correct or not
//        filesystem->readCloseFileStatus(fileName);
//    }
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
//
//#endif