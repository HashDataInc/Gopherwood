/**
 * User: neuyilan@163.com
 * Date: 12/14/17
 * Time: 11:21 AM
 */


#ifndef _TEST_GOPHERWOOD_PREFIX_
#define _TEST_GOPHERWOOD_PREFIX_ "./"
#endif


#include "gtest/gtest.h"
#include "core/gopherwood.h"
#include "core/FileStatus.h"


#include "../../src/core/SharedMemoryManager.h"
#include "../../src/core/FileSystemInter.h"
#include "../../src/core/FileSystemImpl.h"


using namespace Gopherwood;
using namespace Gopherwood::Internal;

class TestSharedMemoryManager : public ::testing::Test {
public:

    TestSharedMemoryManager() {
        printf("Default Constructor of TestSharedMemoryManager");
    }

    ~TestSharedMemoryManager() {
        printf("Default DeConstructor of TestSharedMemoryManager");
    }

protected:
    std::shared_ptr<FileSystemInter> filesystem;
    std::shared_ptr<FileStatus> filestatus;
};

TEST_F(TestSharedMemoryManager, acquireNewBlock) {
    char *fileName = "TestSharedMemoryManager-acquireNewBlock";
    filesystem = std::shared_ptr<FileSystemInter>(new FileSystemImpl(fileName));
    filesystem->createFile(fileName);


    filesystem->acquireNewBlock(fileName);

    filestatus = filesystem->getFileStatus(fileName);
    if(filestatus->getBlockIdVector().size()!=QUOTA_SIZE){
        cout << "error, block vector size != QUOTA_SIZE  :" << endl;
        return;
    }

    cout << "the acquired block id is :" << endl;
    for (int i = 0; i < filestatus->getBlockIdVector().size(); i++) {
        cout << filestatus->getBlockIdVector()[i] << "\t";
    }
    cout<<endl;

    //1->2
    vector<int> tmpVector;
    tmpVector.push_back(filestatus->getBlockIdVector()[0]);
    filesystem->inactiveBlock(fileName,tmpVector);

    //1->0
    tmpVector.clear();
    tmpVector.push_back(filestatus->getBlockIdVector()[1]);
    filesystem->releaseBlock(fileName,tmpVector);

    //2->1
    tmpVector.clear();
    tmpVector.push_back(filestatus->getBlockIdVector()[0]);
    filesystem->evictBlock(fileName,tmpVector);


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
