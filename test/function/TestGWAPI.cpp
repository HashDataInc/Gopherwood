/*
 * gwWrite.cpp
 *
 *  Created on: Jan 30, 2018
 *      Author: houliang
 */

#include <fstream>
#include <iostream>
#include <fcntl.h>
#include <sstream>
#include <cstring>
#include "gopherwood.h"

using namespace std;

char workspace[] = "/media/ephemeral0/goworkspace";
char dataDir[] = "/home/ec2-user/datas/";
gopherwoodFS gwFS;

void initContext() {
    GWContextConfig config;
    config.blockSize = 6*1024*1024;
    config.numBlocks = 100;
//    config.blockSize = 1*1024;
//    config.numBlocks = 10;

    config.numPreDefinedConcurrency = 2;
    config.severity = 3;
    gwFS = gwCreateContext(workspace, &config);
}

void testGWWrite(std::string fileName) {
    std:cout << "hello word" <<std::endl;
    initContext();
    gwFile file = gwOpenFile(gwFS, (char *) fileName.c_str(), GW_CREAT | GW_RDWR);

    int SIZE = 1*1024*1024;

//    int SIZE = 128;
    //3. construct the file name
    std::stringstream ss;
    ss << dataDir << fileName;
    std::string filePath = ss.str();

    //4. read data from file
    std::ifstream infile;
    infile.open(filePath);


    int totalWriteLength = 0;
    char *buf = new char[SIZE];
    infile.read(buf, SIZE);
    int readLengthIn = infile.gcount();
    while (readLengthIn > 0) {
        totalWriteLength += readLengthIn;
//        std::cout << "totalWriteLength=" << totalWriteLength << ",readLength="
//                  << readLengthIn << std::endl;
//        std::cout << "buf=" << buf << std::endl;
        //5. write data to the gopherwood
//        std::cout << "readLengthIn=" << readLengthIn << std::endl;
        gwWrite(gwFS, file, buf, readLengthIn);

//        std::cout << "come in =" << readLengthIn << std::endl;

        buf = new char[SIZE];
        infile.read(buf, SIZE);
        readLengthIn = infile.gcount();
    }

    gwCloseFile(gwFS, file);

    std::cout << "*******END OF WRITE*****, totalWriteLength=" << totalWriteLength << std::endl;

    std::cout << "*******START OF destroyContext*****" << std::endl;
    int res = gwDestroyContext(gwFS);
    std::cout << "*******END OF destroyContext*****,res=" << res << std::endl;
}


void writeUtil(char *fileName, char *buf, int size) {
    std::ofstream ostrm(fileName, std::ios::out | std::ios::app);
    ostrm.write(buf, size);
}

void testGWRead(string fileName) {
    initContext();

    gwFile file = gwOpenFile(gwFS, (char *) fileName.c_str(), GW_RDONLY);


    //3. construct the file name
    std::stringstream ss;
    ss << dataDir << fileName << "-readCache";
    std::string fileNameForWrite = ss.str();


    int SIZE = 1*1024*1024;

//    int SIZE = 128;
    char *readBuf = new char[SIZE];
    int readLength = gwRead(gwFS, file, readBuf, SIZE);

    int totalLength = 0;
    while (readLength > 0) {
        //4. write data to file to check
        totalLength += readLength;
        writeUtil((char *) fileNameForWrite.c_str(), readBuf, readLength);

        readBuf = new char[SIZE];
        readLength = gwRead(gwFS, file, readBuf, SIZE);
//        std::cout << "**************** readLength =  *******************" << readLength << std::endl;
    }


    gwCloseFile(gwFS, file);

    std::cout << "*******END OF READ*****, totalLength=" << totalLength << std::endl;
}


void testGWDelete(string fileName) {
    initContext();

    std::cout << "***********START OF DELETE**************" << std::endl;
    gwDeleteFile(gwFS, (char *) fileName.c_str());
    std::cout << "***********END OF  DELETE**************" << std::endl;
}


void testGWGetFileInfo(string fileName) {
    initContext();
    gwFile file = gwOpenFile(gwFS, (char *) fileName.c_str(), GW_RDONLY);

    GWFileInfo *fileInfo = new GWFileInfo();
    gwStatFile(gwFS, file, fileInfo);

    std::cout << "*****************FILE INFO SIZE =" << fileInfo->fileSize << "*******************" << std::endl;
    gwCloseFile(gwFS, file);
    std::cout << "***********END OF CLOSE FILE OF the testGWGetFileInfo method **************" << std::endl;

}



int main(int agrInt, char **agrStr) {

    int count = 3;

    std::string fileNameArr[count];
    fileNameArr[0] = "TestReadWriteSeek-ReadEvictBlock";
    fileNameArr[1] = "TestReadWriteSeek-WriteBlockWithEvictOtherFileBlock";
    fileNameArr[2] = "TestReadWriteSeek-ThirdThread";


    cout << "********main*******agrInt= " << agrInt << ", agrStr[1]= " << agrStr[1] << endl;

    int timeCount = 1;


    if (strcmp(agrStr[1], "write-1") == 0) {
        for (int i = 0; i < timeCount; i++) {
            testGWWrite(fileNameArr[0]);
        }
    } else if (strcmp(agrStr[1], "write-2") == 0) {
        for (int i = 0; i < timeCount; i++) {
            testGWWrite(fileNameArr[1]);
        }
    } else if (strcmp(agrStr[1], "write-3") == 0) {
        for (int i = 0; i < timeCount; i++) {
            testGWWrite(fileNameArr[2]);
        }
    } else if (strcmp(agrStr[1], "read-1") == 0) {
        testGWRead(fileNameArr[0]);
    } else if (strcmp(agrStr[1], "read-2") == 0) {
        testGWRead(fileNameArr[1]);
    } else if (strcmp(agrStr[1], "read-3") == 0) {
        testGWRead(fileNameArr[2]);
    } else if (strcmp(agrStr[1], "delete-1") == 0) {
        testGWDelete(fileNameArr[0]);
    } else if (strcmp(agrStr[1], "delete-2") == 0) {
        testGWDelete(fileNameArr[1]);
    } else if (strcmp(agrStr[1], "delete-3") == 0) {
        testGWDelete(fileNameArr[2]);
    } else if (strcmp(agrStr[1], "fileInfo-1") == 0) {
        testGWGetFileInfo(fileNameArr[0]);
    } else if (strcmp(agrStr[1], "fileInfo-2") == 0) {
        testGWGetFileInfo(fileNameArr[1]);
    } else if (strcmp(agrStr[1], "fileInfo-3") == 0) {
        testGWGetFileInfo(fileNameArr[2]);
    } else if (strcmp(agrStr[1], "multi-destroy") == 0) {
//        testGWMultiFileWithOneGWFS();
    } else if (strcmp(agrStr[1], "format") == 0) {
        gwFormatContext(workspace);
    }

}


