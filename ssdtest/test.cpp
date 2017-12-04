/*
 * test.cpp
 *
 *  Created on: Nov 20, 2017
 *      Author: root
 */
//
#include "test.h"
//
#include <fcntl.h>
#include <cstdint>
#include <cstdlib>
#include <cstring>
//#include <stdio.h>
#include <sys/ipc.h>
#include <unistd.h>
////#include "stdlib.h"
//#include <bitset>
#include <iostream>
//#include <map>
#include <memory>
//#include <string>
//#include <unordered_map>
//#include "FileStatus.h"
#include "FSConfig.h"
#include <sys/shm.h>
#include <sys/ipc.h>
#include <sys/sem.h>

using namespace std;

void readAndWriteSM() {
	//	SharedMemoryBucket* smBucket;
	//			int flags = O_CREAT | O_RDWR;
	//			char * smPathFileName = "/ssdfile/ssdkv/sharedMemory/smFile";
	//			char *sharedMemoryPath = "/ssdfile/ssdkv/sharedMemory/";
	//			int sm_fd = open(smPathFileName, flags, 0644);
	//			cout << "sm_fd = " << sm_fd << endl;
	//			key_t key = ftok(sharedMemoryPath, 1);
	//			cout << "key = " << key << endl;
	//			cout << "sizeof key is = " << sizeof(key) << endl;
	//			int length = write(sm_fd, &key, sizeof(key));
	//
	//			int sharedMemoryID = shmget(key, SHM_SIZE, IPC_CREAT);
	//			cout << "sharedMemoryID=" << sharedMemoryID << endl;
	//			smBucket = (SharedMemoryBucket *) shmat(sharedMemoryID, NULL, 0);
	//			smBucket->smBitmap = "abcdefg";
	//			cout << "length=" << length << endl;
	//			cout << "smBitmap=" << smBucket->smBitmap << endl;


	//read


	//	SharedMemoryBucket* smBucket;
	//	int flags = O_RDONLY;
	//	char * smPathFileName = "/ssdfile/ssdkv/sharedMemory/smFile";
	//	char *sharedMemoryPath = "/ssdfile/ssdkv/sharedMemory/";
	//	int sm_fd = open(smPathFileName, flags, 0644);
	//	cout << "sm_fd = " << sm_fd << endl;
	//	key_t key = ftok(sharedMemoryPath, 1);
	//	cout << "key = " << key << endl;
	//	cout << "sizeof key is = " << sizeof(key) << endl;
	//	int length = write(sm_fd, &key, sizeof(key));
	//
	//	int sharedMemoryID = 16711683;
	//	smBucket = (SharedMemoryBucket *) shmat(sharedMemoryID, NULL, 0);
	//	smBucket->smBitmap = "abcdefg";
	//	cout << "length=" << length << endl;
	//	cout << "smBitmap=" << smBucket->smBitmap << endl;
}

void bit_set(char *p_data, unsigned char position, int flag) {
	int i = 0;
	if (position > 8 || position < 1 || (flag != 0 && flag != 1)) {
		cout << "输入有误！" << endl;
		return;
	}
	if (flag != (*p_data >> (position - 1) & 1)) {
		*p_data ^= 1 << (position - 1);
	}
}

void createBucket() {
//	char smBucket[100];
	int flags = O_CREAT | O_RDWR;
	char * smPathFileName = "/ssdfile/ssdkv/sharedMemory/smFile";
	char *sharedMemoryPath = "/ssdfile/ssdkv/sharedMemory/";
	int sm_fd = open(smPathFileName, flags, 0644);
	cout << "sm_fd = " << sm_fd << endl;
	key_t key = 5745781;
	cout << "key = " << key << endl;
	cout << "sizeof key is = " << sizeof(key) << endl;
	int length = write(sm_fd, &key, sizeof(key));

	int sharedMemoryID = shmget(key, SHM_SIZE, IPC_CREAT);
	cout << "sharedMemoryID=" << endl;
	cout << sharedMemoryID << endl;
	char * tmp;
	tmp = (char *)malloc(100);
	tmp =  (char*)shmat(sharedMemoryID, NULL, 0);

	memset(tmp, 5, BIT_MAP_SIZE);
	cout << "smBucket->smBitmap: " << tmp << endl;
	cout << "sizeof smBitmap: " << strlen(tmp) << endl;
	for (int i = 0; i < strlen(tmp); i++) {
		char c = tmp[i];
		cout << "before: " << c << "-->";
		for (int j = 7; j >= 0; j--) {
			cout << ((c >> j) & 1);
		}
		cout << endl;
	}
}

void readBucket() {
	char* smBucket;
	int flags = O_RDONLY;
	char * smPathFileName = "/ssdfile/ssdkv/sharedMemory/smFile";
	char *sharedMemoryPath = "/ssdfile/ssdkv/sharedMemory/";
	int sm_fd = open(smPathFileName, flags, 0644);
	cout << "sm_fd = " << sm_fd << endl;
	key_t key = 5745800;
	cout << "key = " << key << endl;
	cout << "sizeof key is = " << sizeof(key) << endl;

	int sharedMemoryID = 17268754;
	smBucket = (char *) shmat(sharedMemoryID, NULL, 0);
	cout << "smBitmap=" << smBucket << endl;
	cout << "strlen(smBucket)=" << strlen(smBucket) << endl;
	for (int i = 0; i < strlen(smBucket); i++) {
		char c = smBucket[i];
		cout << "before: " << c << "-->";
		for (int j = 0; j < 8; j++) {
			cout << ((c >> j) & 1);
		}
		cout << endl;
		;

		for (int j = 0; j < 8; j++) {
			if (((c >> j) & 1)) {
				bit_set(&c, j + 1, 0);
				break;
			}
			//			cout << ((c >> j) & 1);
		}
		memset(smBucket + i, c, 1);
		//		cout << endl;
		cout << "after: " << c << "-->";
		for (int j = 0; j < 8; j++) {
			cout << ((c >> j) & 1);
		}
		cout << endl;
	}

	//				shmdt(static_cast<void *>(smBucket));
	//				shmctl(sharedMemoryID, IPC_RMID, 0);


}

int main() {

	createBucket();
 }

 //void testWrite() {
 //
 //	int flags = O_CREAT | O_RDWR;
 //	int bucketFd = -1;
 //	char buf[100] =
 //			"1234567890abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
 //	char* bucketFilePath = "/ssdfile/ssdkv/gopherwood";
 //	if (bucketFd == -1) {
 //		bucketFd = open(bucketFilePath, flags, 0644);
 //	}
 //
 //	int beginOff = 0;
 //	for (int i = 0; i < 10; i++) {
 //		int res = write(bucketFd, buf + beginOff, 10);
 //		beginOff += 10;
 //	}
 //}

 //int readFile() {
 //	int flags = O_RDONLY;
 //	char * smPathFileName = "/ssdfile/ssdkv/sharedMemory/smFile";
 //	int sm_fd = open(smPathFileName, flags, 0644);
 //	cout << "sm_fd = " << sm_fd << endl;
 //	if (sm_fd < 0) {
 //		return sm_fd;
 //	}
 //	int key;
 //	int length = read(sm_fd, &key, sizeof(key));
 //
 //	cout << "the shared memory key is = " << key << endl;
 //}
 //
 //void testSM() {
 //	SharedMemoryBucket *smBucket = new SharedMemoryBucket();
 //
 //	cout << smBucket->getBitvec() << endl;
 //	uint64_t i = 0;
 //	for (i = 0; i < smBucket->getBitvec().size(); i++) {
 //		cout << "***********" << endl;
 //		if (!smBucket->getBitvec().test(i)) {
 //			cout << "&&&&&&&&&&&&" << endl;
 //			bitset<40> b = smBucket->getBitvec();
 //			b.flip(i);
 //			smBucket->setBitvec(b);
 //			break;
 //		}
 //	}
 //	cout << smBucket->getBitvec() << endl;
 //}
 //
 //void writeFile() {
 //	int flags = O_CREAT | O_RDWR;
 //	char * smPathFileName = "/ssdfile/ssdkv/sharedMemory/smFile";
 //	char *sharedMemoryPath = "/ssdfile/ssdkv/sharedMemory/";
 //
 //	int sm_fd = open(smPathFileName, flags, 0644);
 //	cout << "sm_fd = " << sm_fd << endl;
 //	key_t key = ftok(sharedMemoryPath, 1);
 //	cout << "key = " << key << endl;
 //	cout << "sizeof key is = " << sizeof(key) << endl;
 //	char buf[4];
 //
 //	sprintf(buf, "%d", key);
 //
 //	//	static_cast<char*>
 //	cout << "buf is= " << buf << endl;
 //	int length = write(sm_fd, &key, sizeof(key));
 //	cout << "length=" << length << endl;
 //}

