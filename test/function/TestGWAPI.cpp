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
//#include "../../src/core/gopherwood.h"

using namespace std;

void testGWWrite(std::string fileName) {
    AccessFileType type = AccessFileType::randomType;

    gopherwoodFS gwFS = gwCreateContext((char *) fileName.c_str());
    gwFile file = gwOpenFile(gwFS, (char *) fileName.c_str(), O_CREAT);

    int SIZE = 128;



    //3. construct the file name
    std::stringstream ss;
    ss << "/ssdfile/ssdkv/" << fileName;
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
        std::cout << "totalWriteLength=" << totalWriteLength << ",readLength="
                  << readLengthIn << std::endl;
        std::cout << "buf=" << buf << std::endl;
        //5. write data to the gopherwood
        gwWrite(gwFS, file, buf, readLengthIn);

        buf = new char[SIZE];
        infile.read(buf, SIZE);
        readLengthIn = infile.gcount();
    }

    gwCloseFile(gwFS, file);

    std::cout << "*******END OF WRITE*****, totalWriteLength=" << totalWriteLength << std::endl;
}


void writeUtil(char *fileName, char *buf, int size) {
    std::ofstream ostrm(fileName, std::ios::out | std::ios::app);
    ostrm.write(buf, size);
}

void testGWRead(string fileName) {
    AccessFileType type = AccessFileType::randomType;
    gopherwoodFS gwFS = gwCreateContext((char *) fileName.c_str());
    gwFile file = gwOpenFile(gwFS, (char *) fileName.c_str(), O_RDONLY);


    //3. construct the file name
    std::stringstream ss;
    ss << "/ssdfile/ssdkv/test/" << fileName << "-readCache";
    std::string fileNameForWrite = ss.str();


    int SIZE = 128;

    char *readBuf = new char[SIZE];
    int readLength = gwRead(gwFS, file, readBuf, SIZE);

    int totalLength = 0;
    while (readLength > 0) {
        //4. write data to file to check
        totalLength += readLength;
        writeUtil((char *) fileNameForWrite.c_str(), readBuf, readLength);

        readBuf = new char[SIZE];
        readLength = gwRead(gwFS, file, readBuf, SIZE);
        std::cout << "**************** readLength =  *******************" << readLength << std::endl;
    }


    gwCloseFile(gwFS, file);

    std::cout << "*******END OF READ*****, totalLength=" << totalLength << std::endl;
}


void testGWDelete(string fileName) {
    AccessFileType type = AccessFileType::randomType;
    gopherwoodFS gwFS = gwCreateContext((char *) fileName.c_str());
    gwFile file = gwOpenFile(gwFS, (char *) fileName.c_str(), O_RDONLY);
    std::cout << "***********START OF DELETE**************" << std::endl;
    deleteFile(gwFS, file);
    std::cout << "***********END OF  DELETE**************" << std::endl;
}


int main(int agrInt, char **agrStr) {

    int count = 3;

    std::string fileNameArr[count];
    fileNameArr[0] = "TestReadWriteSeek-ReadEvictBlock";
    fileNameArr[1] = "TestReadWriteSeek-WriteBlockWithEvictOtherFileBlock";
    fileNameArr[2] = "TestReadWriteSeek-ThirdThread";


    cout << "********main*******agrInt= " << agrInt << ", agrStr[1]= " << agrStr[1] << endl;

    int timeCount = 10;


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
    }

    else if (strcmp(agrStr[1], "read-1") == 0) {
        testGWRead(fileNameArr[0]);
    } else if (strcmp(agrStr[1], "read-2") == 0) {
        testGWRead(fileNameArr[1]);
    } else if (strcmp(agrStr[1], "read-3") == 0) {
        testGWRead(fileNameArr[2]);
    }

    else if (strcmp(agrStr[1], "delete-1") == 0) {
        testGWDelete(fileNameArr[0]);
    } else if (strcmp(agrStr[1], "delete-2") == 0) {
        testGWDelete(fileNameArr[1]);
    } else if (strcmp(agrStr[1], "delete-3") == 0) {
        testGWDelete(fileNameArr[2]);
    }


}