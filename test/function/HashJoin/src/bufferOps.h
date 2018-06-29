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
#include "fileOps.h"

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

// opens filename for writing (append mode), and writes size blocks
// starting from pointer buffer

inline uint writeBlocks(char *filename, block_t *buffer, uint size) {
//    int fd = open(filename, O_WRONLY | O_CREAT | O_APPEND, S_IRWXU);

//    char workDir[] = WORK_DIR;
//    GWContextConfig config;
//    config.blockSize = BLOCK_SIZE;;
//    config.numBlocks = NUMBER_OF_BLOCKS;
//    /* create the gopherwood context and file*/
//    gopherwoodFS gwFS = gwCreateContext(workDir, &config);
    gwFile gwfile = gwOpenFile(dbtproj::gwFS, filename, GW_CREAT | GW_RDWR);
    gwSeek(dbtproj::gwFS, gwfile, 0, SEEK_END);

    gwWrite(dbtproj::gwFS, gwfile, buffer, size * sizeof(block_t));


//    write(fd, buffer, size * sizeof (block_t));
//    close(fd);
    gwCloseFile(dbtproj::gwFS, gwfile);
    return size;
}

// writes size blocks starting from pointer buffer to the file described
// by fd file descriptor

inline uint writeBlocks(gopherwoodFS gwFS, gwFile gwfile, block_t *buffer, uint size) {
    gwSeek(gwFS, gwfile, 0, SEEK_END);
    gwWrite(gwFS, gwfile, buffer, size * sizeof(block_t));
    return size;
}

// reads size blocks to buffer

inline uint readBlocks(char *filename, block_t *buffer, uint size) {
//    char workDir[] = WORK_DIR;
//    GWContextConfig config;
//    config.blockSize = BLOCK_SIZE;;
//    config.numBlocks = NUMBER_OF_BLOCKS;
//    /* create the gopherwood context and file*/
//    gopherwoodFS gwFS = gwCreateContext(workDir, &config);
    gwFile gwfile = gwOpenFile(dbtproj::gwFS, filename, GW_CREAT | GW_RDWR);


    struct timeval tv;
    gettimeofday(&tv,NULL);
    int64_t startTime = tv.tv_sec*1000+tv.tv_usec/1000;

    gwRead(dbtproj::gwFS, gwfile, buffer, size * sizeof(block_t));

    gettimeofday(&tv,NULL);
    int64_t endTime = tv.tv_sec*1000+tv.tv_usec/1000;
    dbtproj::totalReadTime += endTime-startTime;

    GWFileInfo *fileInfo = new GWFileInfo();
    gwStatFile(dbtproj::gwFS, gwfile, fileInfo);
    fileOps * fileops = new fileOps();
    std::cout << "gwStatFile. fileName=" << filename << std::endl;
    fileops->printFileInfo(fileInfo);
    gwCloseFile(dbtproj::gwFS,gwfile);
    return size;
}

// reads size blocks to buffer

inline uint readBlocks(gopherwoodFS gwFS ,gwFile gwfile, block_t *buffer, uint size) {
//    read(fd, buffer, size * sizeof(block_t));

    struct timeval tv;
    gettimeofday(&tv,NULL);
    int64_t startTime = tv.tv_sec*1000+tv.tv_usec/1000;

    gwRead(gwFS, gwfile, buffer, size * sizeof(block_t));

    gettimeofday(&tv,NULL);
    int64_t endTime = tv.tv_sec*1000+tv.tv_usec/1000;
    dbtproj::totalReadTime += endTime-startTime;

    return size;
}

#endif
