/**
 * User: Houliang Qi
 * Date: 3/24/18
 * Time: 11:24 PM
 */

#ifndef DBMS_IMPLEMENTATION_HASHJOIN_H
#define DBMS_IMPLEMENTATION_HASHJOIN_H


#include "dbtproj.h"

void
HashJoin(char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile,
         unsigned int *nres, unsigned int *nios);


#endif //DBMS_IMPLEMENTATION_HASHJOIN_H