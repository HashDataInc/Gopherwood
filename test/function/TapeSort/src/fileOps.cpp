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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <assert.h>
#include <cstdlib>
#include "gopherwood.h"
#include "dbtproj.h"

// generates random string



char *getRandomString() {
    char *str;
    str = (char *) malloc(STR_LENGTH);

    const char text[] = "abcdefghijklmnopqrstuvwxyz";

    int i;
    int len = rand() % (STR_LENGTH - 1);
    len += 1;
    for (i = 0; i < len; ++i) {
        str[i] = text[rand() % (sizeof(text) - 1)];
    }
    str[i] = '\0';
    return str;
}

// generates random file

gopherwoodFS initContext() {

    /*********************this is for hashdata master(wuxin)***********************/
    char workDir[] = WORK_DIR;
    GWContextConfig config;
    config.blockSize = BLOCK_SIZE;;
    config.numBlocks = NUMBER_OF_BLOCKS;
    config.numPreDefinedConcurrency = NUM_CONCURRENCY;
    config.severity = SEVERITY;
    /* create the gopherwood context and file*/
    gopherwoodFS gwFS = gwCreateContext(workDir, &config);
    return gwFS;
    /*********************this is for hashdata master(wuxin)***********************/
}


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


void createFromTpcds(char *filename, uint size) {

    std::cout << "[fileOps.create] the sizeof(record_t)=" << sizeof(record_t) << ", sizeof(block_t)=" << sizeof(block_t)
              << std::endl;

    gopherwoodFS gwFS = initContext();
    gwFile gwfile = gwOpenFile(gwFS, filename, GW_CREAT | GW_RDWR);


    std::ifstream inf;
    inf.open(FILE_NAME, std::ifstream::in);
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

                gwWrite(gwFS, gwfile, &block, sizeof(block_t));
                totalCount = 0;
                blockNum++;
            }
        }
    }

    if (totalCount > 0 && totalCount != MAX_RECORDS_PER_BLOCK) {
//        std::cout << "second come in the totalCount == MAX_RECORDS_PER_BLOCK " << std::endl;
        block.nreserved = MAX_RECORDS_PER_BLOCK;
        block.valid = true;
        gwWrite(gwFS, gwfile, &block, sizeof(block_t));
    }

    std::cout << "amountTotalCount=" << amountTotalCount << std::endl;

    GWFileInfo *fileInfo = new GWFileInfo();
    gwStatFile(gwFS, gwfile, fileInfo);
    std::cout << "gwStatFile. fileName=" << filename << std::endl;
    printFileInfo(fileInfo);
    gwCloseFile(gwFS, gwfile);

    inf.close();
}


//void create(char *filename, uint size) {
//
//    std::cout << "[fileOps.create] the sizeof(record_t)=" << sizeof(record_t) << ", sizeof(block_t)=" << sizeof(block_t)
//              << std::endl;
//
//    gopherwoodFS gwFS = initContext();
//    gwFile gwfile = gwOpenFile(gwFS, filename, GW_CREAT | GW_RDWR);
//
//    /*********************this is for hashdata master(wuxin)***********************/
//    gwSeek(gwFS, gwfile, 0, SEEK_END);
//    /*********************this is for hashdata master(wuxin)***********************/
//
//    uint recid = 0;
//    record_t record;
//    block_t block;
//    for (uint b = 0; b < size; ++b) { // for each block
//        block.blockid = b;
//        for (int r = 0; r < MAX_RECORDS_PER_BLOCK; ++r) { // for each record
//
//            // prepare a record
//            record.recid = recid += 1;
//            record.num = rand() % NUM_RANGE;
//            char *bufString = getRandomString();
//            strcpy(record.str, bufString); // put a random string to record
//            free(bufString);
//
//            record.valid = true;
////            if (((double) rand() / (RAND_MAX)) >= 0.02) {
////                record.valid = true;
////            } else {
////                record.valid = false;
////            }
//            memcpy(&block.entries[r], &record, sizeof(record_t)); // copy record to block
//        }
//        block.nreserved = MAX_RECORDS_PER_BLOCK;
//        block.valid = true;
//        gwWrite(gwFS, gwfile, &block, sizeof(block_t));
//    }
//    gwCloseFile(gwFS, gwfile);
//}



// creates one input file
void createFile(char *filename, uint size) {
    srand(time(NULL));
    createFromTpcds(filename, size);
}


void formatGopherwood() {
    /*********************this is for hashdata master(wuxin)***********************/
    char workDir[] = WORK_DIR;
    gwFormatContext(workDir);
    /*********************this is for hashdata master(wuxin)***********************/
}


void printFileInfo(GWFileInfo *fileInfo) {
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


void printFile(char *filename) {

    std::cout << "printFile*******************" << std::endl;
    gopherwoodFS gwFS = initContext();
    gwFile gwfile = gwOpenFile(gwFS, filename, GW_RDONLY);

    block_t block;
    int64_t totalCount = 0;
    while (gwRead(gwFS, gwfile, &block, sizeof(block_t)) > 0) {
        for (uint i = 0; i < block.nreserved; ++i) {
            if (block.entries[i].valid) {
                printf("BL %d, RC %d, %s, %s\n", block.blockid, block.entries[i].recid,
                       block.entries[i].cs_item_sk,
                       block.entries[i].cs_quantity);
                totalCount++;
            }
        }
    }
    std::cout << "[fileOps.printFile]. total read from file =" << filename << ", read count = " << totalCount
              << std::endl;
    gwCloseFile(gwFS, gwfile);
}


int getSize(char *filename) {

    /*********************this is for hashdata master(wuxin)***********************/
    char workDir[] = WORK_DIR;
    GWContextConfig config;
    config.blockSize = BLOCK_SIZE;;
    config.numBlocks = NUMBER_OF_BLOCKS;
    config.numPreDefinedConcurrency = NUM_CONCURRENCY;
    config.severity = SEVERITY;
    /* create the gopherwood context and file*/
    gopherwoodFS gwFS = gwCreateContext(workDir, &config);
    gwFile gwfile = gwOpenFile(gwFS, filename, GW_RDONLY);

    GWFileInfo *gwFileInfo = new GWFileInfo();
    gwStatFile(gwFS, gwfile, gwFileInfo);

    int retSize = gwFileInfo->fileSize / sizeof(block_t);
////    std::cout << "retSize = " << retSize << std::endl;

    gwCloseFile(gwFS, gwfile);
    return retSize;
    /*********************this is for hashdata master(wuxin)***********************/



    /*********************this is for prototype master(qihouliang)***********************/
//    gopherwoodFS gwFS = gwCreateContext();
//    gwFile gwfile = gwOpenFile(gwFS, filename, GW_RDONLY);
//
//    GWFileInfo *gwFileInfo = gwStatFile(gwFS, gwfile);
//
//    int retSize = gwFileInfo->fileSize / sizeof(block_t);
//    gwCloseFile(gwFS, gwfile);
//    return retSize;
    /*********************this is for prototype master(qihouliang)***********************/


}

// returns true if file exists, false otherwise
int exists(char *filename) {
    /*********************this is for hashdata master(wuxin)***********************/
    char workDir[] = WORK_DIR;
    GWContextConfig config;
    config.blockSize = BLOCK_SIZE;;
    config.numBlocks = NUMBER_OF_BLOCKS;
    config.numPreDefinedConcurrency = NUM_CONCURRENCY;
    config.severity = SEVERITY;
    /* create the gopherwood context and file*/
    gopherwoodFS gwFS = gwCreateContext(workDir, &config);
    bool exists = gwFileExists(gwFS, filename);
    return exists;
    /*********************this is for hashdata master(wuxin)***********************/



    /*********************this is for prototype master(qihouliang)***********************/
    /* create the gopherwood context and file*/
//    gopherwoodFS gwFS = gwCreateContext();
//    bool exists = gwCheckFileExist(gwFS, filename);
//    return exists;
    /*********************this is for prototype master(qihouliang)***********************/
}