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
#include "oss/oss.h"
#include "oss/buffer.h"

namespace Gopherwood {
    namespace Internal {
        class QingStoreReadWrite {
        public:
            QingStoreReadWrite();

            ~QingStoreReadWrite();

            void testGetObject();

            int64_t getCurrenttime();

            int64_t qsWrite(char *filename, char *buffer, int32_t size);

            int64_t qsRead(char *filename, char *buffer, int32_t size);

            void testPutObject();

            void initContext();

            void destroyContext();

            void getPutObject(char *filename);

            void getGetObject(char *filename);


            void closePutObject();

            void closeGetObject();

            int64_t qsDeleteObject(char *filename);

            int renameObject(char *beforeFilename, char *afterFilename);


            //TODO. should add the create bucket method.(first ,check wether the bucket exist or not? second, create it)
        private:

            const char *access_key_id = "CNQHLNCMKNSMQXMMTGVL";
            const char *secret_access_key = "RV9HRXHLpcBQe5cSqwZN7i2OBYpmvEO1wXpRugx7";
            const char *location = "pek3a";
            const char *bucket_name = "gopherwood";


//            char qs_ak[100] = {"CNQHLNCMKNSMQXMMTGVL"};
//            char qs_sk[100] = {"RV9HRXHLpcBQe5cSqwZN7i2OBYpmvEO1wXpRugx7"};
//            char qs_loc[20] = {"pek3a"};
            int64_t write_buffer_size = 8 << 20;
            int64_t read_buffer_size = 32 << 20;


            context qsContext;
            ossObject putObject;
            ossObject getObject;

        };

    }


}


#endif //GOPHERWOOD_QINGSTOREREADWRITE_H_H
