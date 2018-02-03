///**
// * User: Houliang Qi
// * Date: 1/17/18
// * Time: 9:56 AM
// */
//
//#ifndef _TEST_GOPHERWOOD_PREFIX_TestRandomReadWriteSeek_
//#define _TEST_GOPHERWOOD_PREFIX_TestRandomReadWriteSeek_ "./"
//
//
//#include "gtest/gtest.h"
//#include "../../src/core/FileSystemInter.h"
//#include "../../src/core/FileSystem.h"
//#include "../../src/core/OutputStreamImpl.h"
//#include "../../src/core/InputStreamImpl.h"
//
//using namespace Gopherwood;
//using namespace Gopherwood::Internal;
//
//class TestRandomReadWriteSeek : public ::testing::Test {
//public:
//
//    TestRandomReadWriteSeek() {
//        printf("Default Constructor of TestRandomReadWriteSeek");
//    }
//
//    ~TestRandomReadWriteSeek() {
//        printf("Default DeConstructor of TestRandomReadWriteSeek");
//    }
//
//    void writeUtil(char *fileName, char *buf, int size) {
//        std::ofstream ostrm(fileName, std::ios::out | std::ios::app);
//        ostrm.write(buf, size);
//    }
//
//    int getRandomIntValue(int start, int end) {
//        int val = (rand() % (end - start + 1)) + start;
//        return val;
//    }
//
//
//protected:
//    std::shared_ptr<FileSystemInter> filesystem;
//    std::shared_ptr<OutputStreamInter> osiImpl;
//    std::shared_ptr<InputStreamInter> isImpl;
//};
//
///**
//TEST_F(TestRandomReadWriteSeek, CloseReadBlockWithoutCache) {
//    int count = 1;
//
//    std::string fileNameArr[count];
//
//    fileNameArr[0] = "TestReadWriteSeek-ReadEvictBlock";
//
//    for (int i = 0; i < count; i++) {
//        char *fileName = (char *) fileNameArr[i].c_str();
//        FileSystem *fs = NULL;
//        fs = new FileSystem(fileName);
//        //1. create context,
//        filesystem = fs->impl->filesystem;
//
//        //7. read the close file status
//        filesystem->readCloseFileStatus(fileName);
//    }
//}
//
//**/
//
//TEST_F(TestRandomReadWriteSeek, CloseReadBlockWithCache) {
//    int count = 1;
//
//    std::string fileNameArr[count];
//    std::string fileNameForWriteArr[count];
//
//
//    fileNameArr[0] = "TestReadWriteSeek-ReadEvictBlock";
//    fileNameForWriteArr[0] = "/ssdfile/ssdkv/TestReadWriteSeek-ReadEvictBlock-readRandomDataWithCache";
//
//
////    fileNameArr[1] = "TestReadWriteSeek-WriteBlockWithEvictOtherFileBlock";
////    fileNameForWriteArr[1] = "/ssdfile/ssdkv/TestReadWriteSeek-WriteBlockWithEvictOtherFileBlock-readCache";
////
////    fileNameArr[2] = "TestReadWriteSeek-ThirdThread";
////    fileNameForWriteArr[2] = "/ssdfile/ssdkv/TestReadWriteSeek-ThirdThread-readCache";
//
//
//
//    for (int i = 0; i < count; i++) {
//
//        FileSystem *fs = NULL;
//        fs = new FileSystem((char *) fileNameArr[i].c_str());
//        //1. create context,
//        filesystem = fs->impl->filesystem;
//
//
//        //2. the buf for read
//        int flag = O_RDWR;
//        int SIZE = 128;
//        char *readBuf = new char[SIZE];
//        int iter = SIZE_OF_BLOCK / SIZE;
//
//
//
//        //3. create inputStream
//        InputStreamImpl *tmpinImpl = new InputStreamImpl(filesystem, fileNameArr[i].c_str(), flag);
//        std::shared_ptr<InputStreamInter> tmpinImplPtr(tmpinImpl);
//        isImpl = tmpinImplPtr;
//
//        //4. get the fileStatus
//        std::shared_ptr<FileStatus> fileStatus = filesystem->getFileStatus(fileNameArr[i].c_str());
//
//        vector<int32_t> blockIDVector = fileStatus->getBlockIdVector();
//        int start = 0;
//        int end = blockIDVector.size() - 1;
//
//
//
//        //5. read data and write the result to file
//        vector<int32_t> randomBlockIDVector;
//        vector<int32_t> randomIndexVector;
//        for (int j = 0; j < 5; j++) {
//            cout << "**********this is the " << j << " block to read" << endl;
//            int randomIndex = getRandomIntValue(start, end);
//            randomIndexVector.push_back(randomIndex);
//            int randomBlockID = blockIDVector[randomIndex];
//            cout << "**********the  randomBlockID is " << randomBlockID << endl;
//            randomBlockIDVector.push_back(randomBlockID);
//
//            // start read data
//            isImpl->seek(randomIndex * SIZE_OF_BLOCK);
//            if (randomBlockID == fileStatus->getLastBucket()) {
//                int64_t endOfBucketOffset = fileStatus->getEndOffsetOfBucket();
//                while (endOfBucketOffset > 0) {
//                    int64_t leftData = SIZE <= endOfBucketOffset ? SIZE : endOfBucketOffset;
//                    int readLength = isImpl->read(readBuf, leftData);
//                    cout << "**********1. the readLength is= " << readLength << endl;
//                    writeUtil((char *) fileNameForWriteArr[i].c_str(), readBuf, readLength);
//                    readBuf = new char[SIZE];
//                    endOfBucketOffset -= readLength;
//                }
//                cout << "**********the last one block to read " << endl;
//            } else {
//                for (int t = 0; t < iter; t++) {
//                    int readLength = isImpl->read(readBuf, SIZE);
//                    cout << "**********2. the readLength is= " << readLength << endl;
//                    writeUtil((char *) fileNameForWriteArr[i].c_str(), readBuf, readLength);
//                    readBuf = new char[SIZE];
//                }
//
//            }
//        }
//
//        //6. read the verify file status to check the random read is right or not?
//        filesystem->readTotalRandomDataFromVerifyFile(randomIndexVector,fileStatus);
//
//        cout << "**************** after the read data *******************" << endl;
//
//        //7. close file
//        filesystem->closeFile((char *) fileNameArr[i].c_str());
//    }
//}
//
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
