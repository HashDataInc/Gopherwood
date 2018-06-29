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

/*
 *  ==========================================================================================
 *  Database Technology 2012-2013
 *  Header file dbtproj.h to be used by your code.
 *  It is not permitted to change this file. You can make changes to perform tests, 
 *  but please rely on the values and typedefs shown below.
 * 
 *  ==========================================================================================
 *
 */


#ifndef _DBTPROJ_H
#define _DBTPROJ_H

#include <string>
#include <cstdlib>

#define STR_LENGTH 32
#define MAX_RECORDS_PER_BLOCK 5000

#define BLOCK_SIZE 64*1024*1024
#define NUMBER_OF_BLOCKS 190
#define NUM_CONCURRENCY  4
#define SEVERITY  2
#define FILE_NAME "/home/ec2-user/datas/catalog_sales.dat"


#define WORK_DIR "/media/ephemeral0/goworkspace"
// This is the definition of a record. Contains three fields, recid, num and str




typedef struct {
    int recid;
    bool valid; // if set, then this block is valid
    char cs_sold_date_sk[STR_LENGTH];
    char cs_sold_time_sk[STR_LENGTH];
    char cs_ship_date_sk[STR_LENGTH];
    char cs_item_sk[STR_LENGTH];
    char cs_quantity[STR_LENGTH];
} record_t;


// This is the definition of a block, which contains a number of fixed-sized records

typedef struct {
    unsigned int blockid;
    unsigned int nreserved; // how many reserved entries
    record_t entries[MAX_RECORDS_PER_BLOCK]; // array of records
    bool valid; // if set, then this block is valid
    unsigned char misc;
    unsigned int next_blockid;
    unsigned int dummy;
} block_t;


class dbtproj {
public:

    static int64_t totalReadTime;

    void MergeSort(char *infile, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile,
                   unsigned int *nsorted_segs, unsigned int *npasses, unsigned int *nios);

    std::string getTimeMergeSort();

};

#endif
