/*
 * fsConfig.h
 *
 *  Created on: Nov 15, 2017
 *      Author: root
 */

#ifndef FSCONFIG_H_
#define FSCONFIG_H_



const int32_t SHM_SIZE =4096;//the size of shared memory
const int32_t SIZE_OF_FILE =20*1024*1024*1024 ;//the size of the SSD file, default is 20GB
const int32_t SIZE_OF_BLOCK =64*1024*1024; //the size of the bucket size
//const int32_t BIT_MAP_SIZE=SIZE_OF_FILE/(SIZE_OF_BLOCK*8);
const int32_t BIT_MAP_SIZE=40;

char *bucketFilePath = "/ssdfile/ssdkv/gopherwood";
char *sharedMemoryPath = "/ssdfile/ssdkv/sharedMemory/";

char *sharedMemoryFileName = "smFile";// the file which save the key of the shared memory


char * smPathFileName = "/ssdfile/ssdkv/sharedMemory/smFile";
#endif /* FSCONFIG_H_ */
