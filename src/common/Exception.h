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
#ifndef _GOPHERWOOD_COMMON_EXCEPTION_H_
#define _GOPHERWOOD_COMMON_EXCEPTION_H_

#include <stdexcept>
#include <string>

namespace Gopherwood {

class GopherwoodException: public std::runtime_error {
public:
    GopherwoodException(const std::string &arg, const char *file, int line, const char *stack);

    ~GopherwoodException() throw () {
    }

    virtual const char *msg() const {
        return detail.c_str();
    }

protected:
    std::string detail;
};

class GopherwoodIOException: public GopherwoodException {
public:
    GopherwoodIOException(const std::string &arg, const char *file, int line, const char *stack) :
            GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodIOException() throw () {
    }
};

class GopherwoodInvalidParmException: public GopherwoodException {
public:
    GopherwoodInvalidParmException(const std::string &arg, const char *file, int line, const char *stack) :
            GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodInvalidParmException() throw () {
    }
};

class GopherwoodNotImplException: public GopherwoodException {
public:
    GopherwoodNotImplException(const std::string &arg, const char *file, int line, const char *stack) :
            GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodNotImplException() throw () {
    }
};

class GopherwoodSyncException: public GopherwoodException {
public:
    GopherwoodSyncException(const std::string &arg, const char *file, int line, const char *stack) :
            GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodSyncException() throw () {
    }
};

class GopherwoodSharedMemException: public GopherwoodException {
public:
    GopherwoodSharedMemException(const std::string &arg, const char *file, int line, const char *stack) :
            GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodSharedMemException() throw () {
    }
};

class GopherwoodCanceled: public GopherwoodException {
public:
    GopherwoodCanceled(const std::string & arg, const char * file, int line, const char * stack) :
            GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodCanceled() throw () {
    }
};

class GopherwoodNetworkException: public GopherwoodIOException {
public:
    GopherwoodNetworkException(const std::string & arg, const char * file, int line,
            const char * stack) :
            GopherwoodIOException(arg, file, line, stack) {
    }

    ~GopherwoodNetworkException() throw () {
    }
};

class GopherwoodNetworkConnectException: public GopherwoodNetworkException {
public:
    GopherwoodNetworkConnectException(const std::string & arg, const char * file, int line,
            const char * stack) :
            GopherwoodNetworkException(arg, file, line, stack) {
    }

    ~GopherwoodNetworkConnectException() throw () {
    }
};

class GopherwoodOSSException: public GopherwoodException {
public:
    GopherwoodOSSException(const std::string &arg, const char *file, int line, const char *stack) :
            GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodOSSException() throw () {
    }
};

class GopherwoodEndOfStream: public GopherwoodIOException {
public:
    GopherwoodEndOfStream(const std::string & arg, const char * file, int line, const char * stack) :
            GopherwoodIOException(arg, file, line, stack) {
    }

    ~GopherwoodEndOfStream() throw () {
    }
};

class GopherwoodTimeoutException: public GopherwoodException {
public:
    GopherwoodTimeoutException(const std::string & arg, const char * file, int line,
            const char * stack) :
            GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodTimeoutException() throw () {
    }
};

}
#endif /* _GOPHERWOOD_COMMON_EXCEPTION_H_ */
