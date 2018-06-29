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

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <iostream>
#include <time.h>
#include <cstdlib>

#include "dbtproj.h"
#include "fileOps.h"
#include "HashJoin.h"

using namespace std;

void printMenu() {
    cout << endl << "Press the key with chosen option: " << endl;
    cout << "1) format gophrewood" << endl;
    cout << "2) Hash Join" << endl;
    cout << "3) Exit" << endl;
    cout << "> ";
}

void initContext() {
    char workDir[] = WORK_DIR;
    GWContextConfig config;
    config.blockSize = BLOCK_SIZE;;
    config.numBlocks = NUMBER_OF_BLOCKS;
    config.numPreDefinedConcurrency = NUM_CONCURRENCY;
    config.severity = SEVERITY;

    /* create the gopherwood context and file*/
    dbtproj::gwFS = gwCreateContext(workDir, &config);

    dbtproj::totalReadTime = 0;
}

void formatGW() {
    dbtproj *dbtproj1 = new dbtproj();
    dbtproj1->formatGopherwood();
    initContext();
}

std::string getTime() {
    time_t timep;
    time(&timep);
    char tmp[64];
    strftime(tmp, sizeof(tmp), "%Y-%m-%d %H:%M:%S", localtime(&timep));
    return tmp;
}

void testHashJoin() {
    char infile1[] = "infile1.bin";
    char infile2[] = "infile2.bin";
    char outfile[] = "output.bin";

    fileOps *fileOps1 = new fileOps();

    std::cout << "[time. ]start to create the two file, time is = " << getTime() << std::endl;
    fileOps1->createTwoFiles(infile1, 6000, infile2, 1000);
    std::cout << "[time. ]end to create the two file, time is = " << getTime() << std::endl;


    std::cout << "[time. ]start to print the file1, time is = " << getTime() << std::endl;
//    fileOps1->printFile(infile1);
    std::cout << "[time. ]end to print the file1, time is = " << getTime() << std::endl;

    std::cout << "[time. ]start to print the file2, time is = " << getTime() << std::endl;
//    fileOps1->printFile(infile2);
    std::cout << "[time. ]end to print the file2, time is = " << getTime() << std::endl;


    uint nmem_blocks = 200;
//    uint nmem_blocks = 10;
    block_t *buffer = (block_t *) malloc(nmem_blocks * sizeof(block_t));
    uint nsorted_segs = 0, npasses = 0, nios = 0, nres = 0, nunique = 0;

    std::cout << "[time. ]start do the hash join job, time is = " << getTime() << std::endl;
    dbtproj *dbtprojInstence = new dbtproj();

    HashJoin(infile1, infile2, 2, buffer, nmem_blocks, outfile, &nres, &nios);
    std::cout << "[time. ]end do the hash join job, time is = " << getTime() << std::endl;

    printf("nios = %d, nres = %d\n", nios, nres);
    std::cout << "[time. ]start to print the output file, time is = " << getTime() << std::endl;
//    fileOps1->printFile(outfile);
    std::cout << "[time. ]end to print the output file, time is = " << getTime() << std::endl;

    std::cout << "[time. ] totalReadTime = " << dbtproj::totalReadTime << " ms, "
            "=" << dbtproj::totalReadTime/1000 << " s, "
            "= "<< dbtproj::totalReadTime/1000/60<<" m" << std::endl;

    free(buffer);

}


int main(int argc, char **argv) {
    char input = 0;
    while (input != '3') {
        do {
            cin.sync();
            cin.clear();
            printMenu();
            cin >> input;
        } while (!cin.fail() && input != '1' && input != '2' && input != '3');

        switch (input) {
            case '1':
                formatGW();
                break;

            case '2':
                testHashJoin();
                break;

            case '3':
                return 0;
        }
    }
    return 0;
}





