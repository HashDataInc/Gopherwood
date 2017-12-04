/*
 * test.h
 *
 *  Created on: Nov 20, 2017
 *      Author: root
 */
/*
 * test.cpp
 *
 *  Created on: Nov 20, 2017
 *      Author: root
 */

#include <unordered_map>

#ifndef TEST_H_
#define TEST_H_

typedef struct {
	char *smBitmap;
} SharedMemoryBucket;
class test {
	int testFile();
	void testSM();
	void bit_set(char *p_data, unsigned char position, int flag);

};

#endif /* TEST_H_ */
