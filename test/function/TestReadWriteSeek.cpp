/**
 * User: neuyilan@163.com
 * Date: 12/14/17
 * Time: 11:21 AM
 */


#ifndef _TEST_GOPHERWOOD_PREFIX_TestReadWriteSeek_
#define _TEST_GOPHERWOOD_PREFIX_TestReadWriteSeek_ "./"


#include "gtest/gtest.h"
#include "../../src/core/FileSystemInter.h"
#include "../../src/core/FileSystem.h"
#include "../../src/core/OutputStreamImpl.h"
#include "../../src/core/InputStreamImpl.h"

using namespace Gopherwood;
using namespace Gopherwood::Internal;

class TestReadWriteSeek : public ::testing::Test {
public:

    TestReadWriteSeek() {
        printf("Default Constructor of TestReadWriteSeek");
    }

    ~TestReadWriteSeek() {
        printf("Default DeConstructor of TestReadWriteSeek");
    }


    void writeUtil(char *fileName, char *buf, int size) {
        std::ofstream ostrm(fileName, std::ios::out|std::ios::app);
        ostrm.write(buf, size);
    }


    int readUtil(char *fileName, char *buf, int size) {
        cout << "************** in the readUtil*************" << endl;
        std::ifstream istrm(fileName, std::ios::in);
        istrm.read(buf, size);
        int readLength = istrm.gcount();
        cout << "readLength = " << readLength << endl;
        return readLength;
    }

protected:
    std::shared_ptr<FileSystemInter> filesystem;
    std::shared_ptr<OutputStreamInter> osiImpl;
    std::shared_ptr<InputStreamInter> isImpl;
};




//TEST_F(TestReadWriteSeek, WriteAcquireNewBlock) {
//    char *fileName = "TestReadWriteSeek-WriteAcquireNewBlock";
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
//    char writeBuf[1024 *
//                  1024] = "WriteAcquireNewBlock,WriteAcquireNewBlock,WriteAcquireNewBlock,WriteAcquireNewBlock,"
//            "WriteAcquireNewBlock,WriteAcquireNewBlock,WriteAcquireNewBlock,WriteAcquireNewBlock,WriteAcquireNewBlock,"
//            "WriteAcquireNewBlock,WriteAcquireNewBlock,WriteAcquireNewBlock,WriteAcquireNewBlock,WriteAcquireNewBlock,"
//            "WriteAcquireNewBlock,WriteAcquireNewBlock,WriteAcquireNewBlock,WriteAcquireNewBlock,WriteAcquireNewBlock,"
//            "WriteAcquireNewBlock,WriteAcquireNewBlock,WriteAcquireNewBlock,WriteAcquireNewBlock,WriteAcquireNewBlock";
//    for (int i = 0; i < 5; i++) {
//        osiImpl->write(writeBuf, strlen(writeBuf));
//    }
//
//    //5. close file
//    filesystem->closeFile(fileName);
//    //7. read the close file status
//    filesystem->readCloseFileStatus(fileName);
//}


//TEST_F(TestReadWriteSeek, ReadAcquireNewBlock) {
//    char *fileName = "TestReadWriteSeek-ReadAcquireNewBlock";
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
//             1024] = "ReadAcquireNewBlock,ReadAcquireNewBlock,ReadAcquireNewBlock,ReadAcquireNewBlock,"
//            "ReadAcquireNewBlock,ReadAcquireNewBlock,ReadAcquireNewBlock,ReadAcquireNewBlock,ReadAcquireNewBlock,"
//            "ReadAcquireNewBlock,ReadAcquireNewBlock,ReadAcquireNewBlock,ReadAcquireNewBlock,ReadAcquireNewBlock,"
//            "ReadAcquireNewBlock,ReadAcquireNewBlock,ReadAcquireNewBlock,ReadAcquireNewBlock,ReadAcquireNewBlock,"
//            "ReadAcquireNewBlock,ReadAcquireNewBlock,ReadAcquireNewBlock,ReadAcquireNewBlock,ReadAcquireNewBlock";
//    for (int i = 0; i < 5; i++) {
//        osiImpl->write(buf, strlen(buf));
//    }
//
//    //5. create input stream
//    InputStreamImpl *tmpinImpl = new InputStreamImpl(filesystem, fileName, flag);
//    std::shared_ptr<InputStreamInter> tmpinImplPtr(tmpinImpl);
//    isImpl = tmpinImplPtr;
//
//    //6. read data
//    char readBuf[SIZE_OF_BLOCK / 8];
//    int readLength = isImpl->read(readBuf, sizeof(readBuf));
//    cout << "**************** the read data  *******************" << endl;
//    int totalLength = 0;
//    while (readLength > 0) {
//        totalLength += readLength;
//        cout << endl << "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& readLength = " << readLength << ", totalLength="
//             << totalLength
//             << endl;
//        cout << readBuf;
//        readLength = isImpl->read(readBuf, sizeof(readBuf));
//    }
//    cout << endl;
//    cout << "**************** the read data *******************" << endl;
//
//
//    //6. close file
//    filesystem->closeFile(fileName);
//    //7. read the close file status
//    filesystem->readCloseFileStatus(fileName);
//}


/**

TEST_F(TestReadWriteSeek, WriteEvictBlock) {
    char *fileName = "TestReadWriteSeek-WriteEvictBlock";
    int flag = O_RDWR;
    FileSystem *fs = NULL;
    fs = new FileSystem(fileName);
    //1. create context,
    filesystem = fs->impl->filesystem;

    //2. create file
    filesystem->createFile(fileName);

    //3. create output stream
    OutputStreamImpl *tmpImpl = new OutputStreamImpl(filesystem, fileName, flag);
    std::shared_ptr<OutputStreamInter> tmposiImpl(tmpImpl);
    osiImpl = tmposiImpl;

    //4. write data
    char buf[1024 *
             1024] = "WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,"
            "WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,"
            "WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,"
            "WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,WriteEvictBlock,WriteEvictBlock";
    for (int i = 0; i < 15; i++) {
        osiImpl->write(buf, strlen(buf));
    }

    //6. close file
    filesystem->closeFile(fileName);
    //7. read the close file status
    filesystem->readCloseFileStatus(fileName);
}

 **/

TEST_F(TestReadWriteSeek, ReadEvictBlock) {
    char *fileName = "TestReadWriteSeek-ReadEvictBlock";
    int flag = O_RDWR;
    FileSystem *fs = NULL;
    fs = new FileSystem(fileName);
    //1. create context,
    filesystem = fs->impl->filesystem;

    //2. create file
    filesystem->createFile(fileName);

    //3. create output stream
    OutputStreamImpl *tmpImpl = new OutputStreamImpl(filesystem, fileName, flag);
    std::shared_ptr<OutputStreamInter> tmposiImpl(tmpImpl);
    osiImpl = tmposiImpl;


    std::ifstream infile;
    char buf[128];
    infile.open("/ssdfile/ssdkv/TestReadWriteSeek-ReadEvictBlock");
    int totalWriteLength = 0;
    while (!infile.eof()) {
        totalWriteLength += strlen(buf);
        cout << "totalWriteLength=" << totalWriteLength << endl;
        infile >> buf;
        osiImpl->write(buf, strlen(buf));
    }


//    5. create input stream
    InputStreamImpl *tmpinImpl = new InputStreamImpl(filesystem, fileName, flag);
    std::shared_ptr<InputStreamInter> tmpinImplPtr(tmpinImpl);
    isImpl = tmpinImplPtr;

    //6. read data
    char readBuf[SIZE_OF_BLOCK / 8];

    int readLength = isImpl->read(readBuf, sizeof(readBuf));
    cout << "**************** before the read data  *******************" << endl;

    char *fileNameForWrite = "/ssdfile/ssdkv/TestReadWriteSeek-ReadEvictBlock-read";
    int totalLength = 0;
    while (readLength > 0) {
        totalLength += readLength;
        writeUtil(fileNameForWrite, readBuf, readLength);
        readLength = isImpl->read(readBuf, sizeof(readBuf));
    }
    cout << "**************** after the read data *******************" << endl;

    //6. close file
    filesystem->closeFile(fileName);
    //7. read the close file status
    filesystem->readCloseFileStatus(fileName);
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
#ifdef DATA_DIR
    if (0 != chdir(DATA_DIR)) {
        abort();
    }
#endif
    return RUN_ALL_TESTS();
}

#endif