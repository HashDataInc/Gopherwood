/*
* DBMS Implementation
* Copyright (C) 2013 George Piskas, George Economides
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation; either version 2 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License along
* with this program; if not, write to the Free Software Foundation, Inc.,
* 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*
* Contact: geopiskas@gmail.com
*/

#include "fileOps.h"

#include <string.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <vector>

// generates random string



std::vector<std::string> split(std::string &str, std::string &pattern) {
    char *strc = new char[strlen(str.c_str()) + 1];
    strcpy(strc, str.c_str());
    std::vector<std::string> resultVec;
    char *tmpStr = strtok(strc, pattern.c_str());
    while (tmpStr != NULL) {
        resultVec.push_back(std::string(tmpStr));
        tmpStr = strtok(NULL, pattern.c_str());
    }

    delete[] strc;

    if (resultVec.size() > 18) {
//        std::cout << "resultVec size()=" << resultVec.size() << ", cs_quantity=" << resultVec[18] << std::endl;
    }

    return resultVec;
}


void fileOps::printFileInfo(GWFileInfo *fileInfo) {
    std::cout << "gwStatFile printFileInfo. "
              << "fileSize=" << fileInfo->fileSize
              << " ,curQuota=" << fileInfo->curQuota
              << " ,maxQuota=" << fileInfo->maxQuota
              << " ,numActivated=" << fileInfo->numActivated
              << " ,numBlocks=" << fileInfo->numBlocks
              << " ,numEvicted=" << fileInfo->numEvicted
              << " ,numLoaded=" << fileInfo->numLoaded
//              << " ,numReadMiss=" << fileInfo->numReadMiss
//              << " ,numTotalRead=" << fileInfo->numTotalRead
//              << " ,numWaitLoading=" << fileInfo->numWaitLoading
              << std::endl;
}


void createFromTpcds1(char *filename, uint size) {

    std::cout << "[fileOps.create] the sizeof(record_t)=" << sizeof(record_t) << ", sizeof(block_t)=" << sizeof(block_t)
              << std::endl;

    gwFile gwfile = gwOpenFile(dbtproj::gwFS, filename, GW_CREAT | GW_RDWR);

    /*********************this is for hashdata master(wuxin)***********************/
    gwSeek(dbtproj::gwFS, gwfile, 0, SEEK_END);
    /*********************this is for hashdata master(wuxin)***********************/


    std::ifstream inf;
    inf.open(FILE_NAME_1, std::ifstream::in);
    std::string line;
    std::string delim = "|";

    int64_t totalCount = 0;
    int64_t blockNum = 0;
    record_t record;
    block_t block;
    std::vector<std::string> resultVec;
    int64_t amountTotalCount = 0;
    while (!inf.eof()) {
        block.blockid = blockNum;
        getline(inf, line);
        amountTotalCount++;
        resultVec = split(line, delim);
//        std::cout << "line = " << line << std::endl;

        if (resultVec.size() > 19) {
            record.recid = totalCount;
            strcpy(record.cs_sold_date_sk, resultVec[0].c_str());
            strcpy(record.cs_sold_time_sk, resultVec[1].c_str());
            strcpy(record.cs_ship_date_sk, resultVec[2].c_str());
            strcpy(record.cs_item_sk, resultVec[15].c_str());
            strcpy(record.cs_quantity, resultVec[18].c_str());
            record.valid = true;

            memcpy(&block.entries[totalCount], &record, sizeof(record_t)); // copy record to block

            totalCount++;

            if (totalCount == MAX_RECORDS_PER_BLOCK) {
//                std::cout << "come in the totalCount == MAX_RECORDS_PER_BLOCK " << std::endl;
                block.nreserved = MAX_RECORDS_PER_BLOCK;
                block.valid = true;

                gwWrite(dbtproj::gwFS, gwfile, &block, sizeof(block_t));
                totalCount = 0;
                blockNum++;
            }
        }
    }

    if (totalCount > 0 && totalCount != MAX_RECORDS_PER_BLOCK) {
//        std::cout << "second come in the totalCount == MAX_RECORDS_PER_BLOCK " << std::endl;
        block.nreserved = MAX_RECORDS_PER_BLOCK;
        block.valid = true;
        gwWrite(dbtproj::gwFS, gwfile, &block, sizeof(block_t));
    }

    std::cout << "amountTotalCount=" << amountTotalCount << std::endl;

    GWFileInfo *fileInfo = new GWFileInfo();
    gwStatFile(dbtproj::gwFS, gwfile, fileInfo);
    std::cout << "gwStatFile. fileName=" << filename << std::endl;
    fileOps *fileops = new fileOps();
    fileops->printFileInfo(fileInfo);
    gwCloseFile(dbtproj::gwFS, gwfile);

//    delete fileInfo;
    inf.close();
}


void createFromTpcds2(char *filename, uint size) {

    std::cout << "[fileOps.create] createFromTpcds2 the sizeof(record_t)=" << sizeof(record_t) << ", sizeof(block_t)="
              << sizeof(block_t)
              << std::endl;

    gwFile gwfile = gwOpenFile(dbtproj::gwFS, filename, GW_CREAT | GW_RDWR);

    /*********************this is for hashdata master(wuxin)***********************/
    gwSeek(dbtproj::gwFS, gwfile, 0, SEEK_END);
    /*********************this is for hashdata master(wuxin)***********************/


    std::ifstream inf;
    inf.open(FILE_NAME_2, std::ifstream::in);
    std::string line;
    std::string delim = "|";

    int64_t totalCount = 0;
    int64_t blockNum = 0;
    record_t record;
    block_t block;
    std::vector<std::string> resultVec;
    int64_t amountTotalCount = 0;

    std::cout << "come in 11111111111111111111" << std::endl;
    while (!inf.eof()) {
        block.blockid = blockNum;
        getline(inf, line);
        amountTotalCount++;
        resultVec = split(line, delim);
//        std::cout << "line = " << line << std::endl;

        if (resultVec.size() > 19) {
            record.recid = totalCount;
            strcpy(record.cs_item_sk, resultVec[0].c_str());
            strcpy(record.cs_sold_date_sk, resultVec[1].c_str());
            strcpy(record.cs_sold_time_sk, resultVec[2].c_str());
            strcpy(record.cs_ship_date_sk, resultVec[2].c_str());
            strcpy(record.cs_quantity, resultVec[18].c_str());
            record.valid = true;

            memcpy(&block.entries[totalCount], &record, sizeof(record_t)); // copy record to block

            totalCount++;

            if (totalCount == MAX_RECORDS_PER_BLOCK) {
//                std::cout << "come in the totalCount == MAX_RECORDS_PER_BLOCK " << std::endl;
                block.nreserved = MAX_RECORDS_PER_BLOCK;
                block.valid = true;

                gwWrite(dbtproj::gwFS, gwfile, &block, sizeof(block_t));
                totalCount = 0;
                blockNum++;
            }
        }
    }

    if (totalCount > 0 && totalCount != MAX_RECORDS_PER_BLOCK) {
//        std::cout << "second come in the totalCount == MAX_RECORDS_PER_BLOCK " << std::endl;
        block.nreserved = MAX_RECORDS_PER_BLOCK;
        block.valid = true;
        gwWrite(dbtproj::gwFS, gwfile, &block, sizeof(block_t));
    }

    std::cout << "amountTotalCount=" << amountTotalCount << std::endl;

    GWFileInfo *fileInfo = new GWFileInfo();
    gwStatFile(dbtproj::gwFS, gwfile, fileInfo);
    std::cout << "gwStatFile. fileName=" << filename << std::endl;
    fileOps *fileops = new fileOps();
    fileops->printFileInfo(fileInfo);
    gwCloseFile(dbtproj::gwFS, gwfile);

    inf.close();
}


// creates two input files
void fileOps::createTwoFiles(char *filename1, uint size1, char *filename2, uint size2) {
    srand(time(NULL));
    createFromTpcds1(filename1, size1);
    createFromTpcds2(filename2, size2);
}


void fileOps::printFile(char *filename) {

    std::cout << "printFile*******************" << std::endl;
    gwFile gwfile = gwOpenFile(dbtproj::gwFS, filename, GW_RDONLY);

    block_t block;
    int64_t totalCount = 0;
    while (gwRead(dbtproj::gwFS, gwfile, &block, sizeof(block_t)) > 0) {
        for (uint i = 0; i < block.nreserved; ++i) {
            if (block.entries[i].valid) {
//                printf("BL %d, RC %d, %s, %s\n", block.blockid, block.entries[i].recid,
//                       block.entries[i].cs_item_sk,
//                       block.entries[i].cs_quantity);
                totalCount++;
            }
        }
    }
    std::cout << "[fileOps.printFile]. total read from file =" << filename << ", read count = " << totalCount
              << std::endl;
    gwCloseFile(dbtproj::gwFS, gwfile);
}


int fileOps::getSize(char *filename) {
//    struct stat st;
//    stat(filename, &st);
//    return st.st_size / sizeof(block_t);

//    std::cout << "COME IN THE getSize method" << std::endl;
//    char workDir[] = WORK_DIR;
//    GWContextConfig config;
//    config.blockSize = BLOCK_SIZE;;
//    config.numBlocks = NUMBER_OF_BLOCKS;
//    /* create the gopherwood context and file*/
//    gopherwoodFS gwFS = gwCreateContext(workDir, &config);
    gwFile gwfile = gwOpenFile(dbtproj::gwFS, filename, GW_RDONLY);

    GWFileInfo *gwFileInfo = new GWFileInfo();
    gwStatFile(dbtproj::gwFS, gwfile, gwFileInfo);
    int retSize = gwFileInfo->fileSize / sizeof(block_t);
//    std::cout << "retSize = " << retSize << std::endl;

    gwCloseFile(dbtproj::gwFS, gwfile);
    return retSize;


}

// returns true if file exists, false otherwise

int fileOps::exists(char *filename) {
//    struct stat st;
//    return stat(filename, &st) == 0;

//    char workDir[] = WORK_DIR;
//    GWContextConfig config;
//    config.blockSize = BLOCK_SIZE;;
//    config.numBlocks = NUMBER_OF_BLOCKS;
//    /* create the gopherwood context and file*/
//    gopherwoodFS gwFS = gwCreateContext(workDir, &config);
    bool exists = gwFileExists(dbtproj::gwFS, filename);
    return exists;
}