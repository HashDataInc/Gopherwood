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

#ifndef BUFFEROPS_H
#define    BUFFEROPS_H

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <iostream>
#include <sys/time.h>

#include "dbtproj.h"
#include "gopherwood.h"

// empties a block
inline void emptyBlock(block_t *buffer) {
    for (int i = 0; i < MAX_RECORDS_PER_BLOCK; i++) {
        (*buffer).entries[i].valid = false;
    }
    (*buffer).nreserved = 0;
}

// empties the whole buffer
inline void emptyBuffer(block_t *buffer, uint size) {
    for (uint i = 0; i < size; i++) {
        emptyBlock(buffer + i);
        buffer[i].valid = true;
    }
}


// writes size blocks starting from pointer buffer to the file described
// by fd file descriptor
inline uint writeBlocks(gopherwoodFS gwFS, gwFile gwfile, block_t *buffer, uint size) {
    /*********************this is for hashdata master(wuxin)***********************/
    gwSeek(gwFS, gwfile, 0, SEEK_END);
    /*********************this is for hashdata master(wuxin)***********************/
    gwWrite(gwFS, gwfile, buffer, size * sizeof(block_t));
    return size;
}


// reads size blocks to buffer
inline uint readBlocks(gopherwoodFS gwFS, gwFile gwfile, block_t *buffer, uint size) {


    struct timeval tv;
    gettimeofday(&tv,NULL);
    int64_t startTime = tv.tv_sec*1000+tv.tv_usec/1000;

    int64_t tobeReadLength = size * sizeof(block_t);
    int64_t haveReadLength = 0;
    int64_t readOffset = 0;
    while (tobeReadLength > 0) {
        haveReadLength = gwRead(gwFS, gwfile, buffer + readOffset, tobeReadLength);
        tobeReadLength -= haveReadLength;
        readOffset += haveReadLength;
    }
    if (readOffset != size * sizeof(block_t)) {
        std::cout << "readBlocks. actual read length = " << readOffset << ", target read length = "
                  << size * sizeof(block_t)
                  << std::endl;
    }

    gettimeofday(&tv,NULL);
    int64_t endTime = tv.tv_sec*1000+tv.tv_usec/1000;
    dbtproj::totalReadTime += endTime-startTime;

    return size;
}


// reads size blocks to buffer from a specific point (offset) of the file

inline uint preadBlocks(gopherwoodFS gwFS, gwFile gwfile, block_t *buffer, uint offset, uint size) {

    struct timeval tv;
    gettimeofday(&tv,NULL);
    int64_t startTime = tv.tv_sec*1000+tv.tv_usec/1000;

    gwSeek(gwFS, gwfile, offset * sizeof(block_t), SEEK_SET);
    int64_t tobeReadLength = size * sizeof(block_t);
    int64_t haveReadLength = 0;
    int64_t readOffset = 0;
    while (tobeReadLength > 0) {
        haveReadLength = gwRead(gwFS, gwfile, buffer + readOffset, tobeReadLength);
        tobeReadLength -= haveReadLength;
        readOffset += haveReadLength;
    }
    if (readOffset != size * sizeof(block_t)) {
        std::cout << "preadBlocks. actual read length = " << readOffset << ", target read length = "
                  << size * sizeof(block_t)
                  << std::endl;
    }

    gettimeofday(&tv,NULL);
    int64_t endTime = tv.tv_sec*1000+tv.tv_usec/1000;
    dbtproj::totalReadTime += endTime-startTime;

    return size;
}

#endif
