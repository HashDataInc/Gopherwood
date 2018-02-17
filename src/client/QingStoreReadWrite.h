/********************************************************************
 * 2017 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
