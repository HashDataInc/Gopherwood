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

#include "gopherwood.h"
#include <cstdlib>

#include <string>

#define STR_LENGTH 32
#define MAX_RECORDS_PER_BLOCK 5000
#define NUM_RANGE 10000000

#define BLOCK_SIZE 64*1024*1024
#define NUMBER_OF_BLOCKS 27
#define NUM_CONCURRENCY  5
#define SEVERITY  2


//#define BLOCK_SIZE 1024*1024
//#define NUMBER_OF_BLOCKS 50
//#define NUM_CONCURRENCY  3

#define FILE_NAME_1 "/home/ec2-user/datas/catalog_sales.dat"

#define FILE_NAME_2 "/home/ec2-user/datas/item.dat"


/* for function test */
//#define STR_LENGTH 120
//#define MAX_RECORDS_PER_BLOCK 100
//#define NUM_RANGE 10000
//
//#define BLOCK_SIZE 1*1024*1024
//#define NUMBER_OF_BLOCKS 10



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
    static gopherwoodFS gwFS;
    static int64_t totalReadTime;

    dbtproj() {};

    ~dbtproj() {};

    void formatGopherwood();




/* ----------------------------------------------------------------------------------------------------------------------
   infile1: the name of the first input file
   infile2: the name of the second input file
   field: which field will be used for the join: 0 is for recid, 1 is for num, 2 is for str and 3 is for both num and str
   buffer: pointer to memory buffer
   nmem_blocks: number of blocks in memory
   outfile: the name of the output file
   nres: number of pairs in output (this should be set by you)
   nios: number of IOs performed (this should be set by you)
   ----------------------------------------------------------------------------------------------------------------------
 */
//    void
//    HashJoin(char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks,
//             char *outfile,
//             unsigned int *nres, unsigned int *nios);
};


#endif
