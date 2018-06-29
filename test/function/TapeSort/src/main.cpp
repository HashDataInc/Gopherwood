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

using namespace std;

void printMenu() {
    cout << endl << "Press the key with chosen option: " << endl;
    cout << "1) format gophrewood" << endl;
    cout << "2) TapeSort" << endl;
    cout << "3) Exit" << endl;
    cout << "> ";
}

void formatGW() {
    formatGopherwood();
    dbtproj::totalReadTime = 0;
}


std::string getTime() {
    time_t timep;
    time(&timep);
    char tmp[64];
    strftime(tmp, sizeof(tmp), "%Y-%m-%d %H:%M:%S", localtime(&timep));
    return tmp;
}


void testMergeSort() {

    char infile1[] = "infile1-tapeSort.bin";
    char outfile[] = "output-tapeSort.bin";

    std::cout << "[time. ]start to create the file, time is = " << getTime() << std::endl;
    createFile(infile1, 8000);
    std::cout << "[time. ]end to create the file, time is = " << getTime() << std::endl;


    std::cout << "[time. ]start to print the input file, time is = " << getTime() << std::endl;
//    printFile(infile1);
    std::cout << "[time. ]end to print the input file, time is = " << getTime() << std::endl;

    uint nmem_blocks = 200;
    block_t *buffer = (block_t *) malloc(nmem_blocks * sizeof(block_t));
    uint nsorted_segs = 0, npasses = 0, nios = 0, nres = 0, nunique = 0;


    std::cout << "[time. ]start to merge sort. time is = " << getTime() << std::endl;
    dbtproj* dbtproj1 = new dbtproj();
    dbtproj1->MergeSort(infile1, 1, buffer, nmem_blocks, outfile, &nsorted_segs, &npasses, &nios);
    std::cout << "[time. ]end to merge sort. time is = " << getTime() << std::endl;

    free(buffer);
    printf("nios = %d, npasses = %d, nsorted_segs = %d\n", nios, npasses, nsorted_segs);

    std::cout << "[time. ] totalReadTime = " << dbtproj::totalReadTime << " ms, "
            "=" << dbtproj::totalReadTime / 1000 << " s, "
            "= " << dbtproj::totalReadTime / 1000 / 60 << " m" << std::endl;

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
                testMergeSort();
                break;

            case '3':
                return 0;
        }
    }
    return 0;
}

