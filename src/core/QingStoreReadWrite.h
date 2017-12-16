/**
 * User: neuyilan@163.com
 * Date: 12/15/17
 * Time: 2:17 PM
 */

#ifndef GOPHERWOOD_QINGSTOREREADWRITE_H_H
#define GOPHERWOOD_QINGSTOREREADWRITE_H_H


#include <cstdint>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "qingstor/qingstor.h"

namespace Gopherwood {
    namespace Internal {
        class QingStoreReadWrite {
        public:
            QingStoreReadWrite();

            ~QingStoreReadWrite();

            void qsRead(char *filename);

            void testGetObject();

            int64_t getCurrenttime();

            void qsWrite(char *filename, char *buffer, int32_t size);

            void testPutObject();

            void initContext();

            void destroyContext();

            void getPutObject(char *filename);

            void getGetObject(char *filename);

            void closePutObject();

            void closeGetObject();

        private:

            const char *access_key_id = "CNQHLNCMKNSMQXMMTGVL";
            const char *secret_access_key = "RV9HRXHLpcBQe5cSqwZN7i2OBYpmvEO1wXpRugx7";
            const char *location = "pek3a";
            const char *bucket_name = "gopherwood";

            int64_t file_nums = 16;

            qingstorContext qsContext;
            qingstorObject putObject;
            qingstorObject getObject;

        };

    }


}


#endif //GOPHERWOOD_QINGSTOREREADWRITE_H_H
