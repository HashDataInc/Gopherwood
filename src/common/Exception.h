/********************************************************************
 * 2016 -
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
    GopherwoodException(const std::string & arg, const char * file, int line,
                  const char * stack);

    ~GopherwoodException() throw () {
    }

    virtual const char * msg() const {
        return detail.c_str();
    }

protected:
    std::string detail;
};

class GopherwoodIOException: public std::runtime_error {
public:
    GopherwoodIOException(const std::string & arg, const char * file, int line,
                    const char * stack);

    ~GopherwoodIOException() throw () {
    }

    virtual const char * msg() const {
        return detail.c_str();
    }

protected:
    std::string detail;
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

class GopherwoodRetryException: public GopherwoodIOException {
public:
	GopherwoodRetryException(const std::string & arg, const char * file, int line,
                         const char * stack) :
        GopherwoodIOException(arg, file, line, stack) {
    }

    ~GopherwoodRetryException() throw () {
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

class AccessControlException: public GopherwoodException {
public:
    AccessControlException(const std::string & arg, const char * file, int line,
                           const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~AccessControlException() throw () {
    }
};

class AlreadyBeingCreatedException: public GopherwoodException {
public:
    AlreadyBeingCreatedException(const std::string & arg, const char * file,
                                 int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~AlreadyBeingCreatedException() throw () {
    }
};

class BlockAlreadyExistsException: public GopherwoodException {
public:
	BlockAlreadyExistsException(const std::string & arg, const char * file,
                                 int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~BlockAlreadyExistsException() throw () {
    }
};

class BlockDoesNotExistException: public GopherwoodException {
public:
	BlockDoesNotExistException(const std::string & arg, const char * file,
                                 int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~BlockDoesNotExistException() throw () {
    }
};

class BlockInfoException: public GopherwoodException {
public:
	BlockInfoException(const std::string & arg, const char * file,
                                 int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~BlockInfoException() throw () {
    }
};

class ConnectionFailedException: public GopherwoodException {
public:
	ConnectionFailedException(const std::string & arg, const char * file,
                                 int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~ConnectionFailedException() throw () {
    }
};

class DependencyDoesNotExistException: public GopherwoodException {
public:
	DependencyDoesNotExistException(const std::string & arg, const char * file,
                                 int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~DependencyDoesNotExistException() throw () {
    }
};

class FileAlreadyExistsException: public GopherwoodException {
public:
    FileAlreadyExistsException(const std::string & arg, const char * file,
                               int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~FileAlreadyExistsException() throw () {
    }
};

class FileDoesNotExistException: public GopherwoodException {
public:
	FileDoesNotExistException(const std::string & arg, const char * file, int line,
                          const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~FileDoesNotExistException() throw () {
    }
};

class DirectoryNotEmptyException: public GopherwoodException {
public:
	DirectoryNotEmptyException(const std::string & arg, const char * file, int line,
                          const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~DirectoryNotEmptyException() throw () {
    }
};

class FailedToCheckpointException: public GopherwoodException {
public:
	FailedToCheckpointException(const std::string & arg, const char * file, int line,
                          const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~FailedToCheckpointException() throw () {
    }
};

class FileAlreadyCompletedException: public GopherwoodException {
public:
	FileAlreadyCompletedException(const std::string & arg, const char * file, int line,
                          const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~FileAlreadyCompletedException() throw () {
    }
};

class InvalidFileSizeException: public GopherwoodException {
public:
	InvalidFileSizeException(const std::string & arg, const char * file, int line,
                          const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~InvalidFileSizeException() throw () {
    }
};

class InvalidPathException: public GopherwoodException {
public:
	InvalidPathException(const std::string & arg, const char * file, int line,
                          const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~InvalidPathException() throw () {
    }
};

class InvalidWorkerStateException: public GopherwoodException {
public:
	InvalidWorkerStateException(const std::string & arg, const char * file, int line,
                          const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~InvalidWorkerStateException() throw () {
    }
};

class LineageDeletionException: public GopherwoodException {
public:
	LineageDeletionException(const std::string & arg, const char * file, int line,
                          const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~LineageDeletionException() throw () {
    }
};

class LineageDoesNotExistException: public GopherwoodException {
public:
	LineageDoesNotExistException(const std::string & arg, const char * file, int line,
                          const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~LineageDoesNotExistException() throw () {
    }
};

class NoWorkerException: public GopherwoodException {
public:
	NoWorkerException(const std::string & arg, const char * file, int line,
                          const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~NoWorkerException() throw () {
    }
};

class WorkerOutOfSpaceException: public GopherwoodException {
public:
	WorkerOutOfSpaceException(const std::string & arg, const char * file, int line,
                          const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~WorkerOutOfSpaceException() throw () {
    }
};


class GopherwoodCanceled: public GopherwoodIOException {
public:
	GopherwoodCanceled(const std::string & arg, const char * file, int line,
                    const char * stack) :
        GopherwoodIOException(arg, file, line, stack) {
    }

    ~GopherwoodCanceled() throw () {
    }
};

class GopherwoodEndOfStream: public GopherwoodIOException {
public:
    GopherwoodEndOfStream(const std::string & arg, const char * file, int line,
                    const char * stack) :
        GopherwoodIOException(arg, file, line, stack) {
    }

    ~GopherwoodEndOfStream() throw () {
    }
};

/**
 * Fatal error during the rpc call. It may wrap other exceptions.
 */
class GopherwoodRpcException: public GopherwoodIOException {
public:
    GopherwoodRpcException(const std::string & arg, const char * file, int line,
                     const char * stack) :
        GopherwoodIOException(arg, file, line, stack) {
    }

    ~GopherwoodRpcException() throw () {
    }
};

/**
 * Server throw an error during the rpc call.
 * It should be used internally and parsed for details.
 */
class GopherwoodRpcServerException: public GopherwoodIOException {
public:
    GopherwoodRpcServerException(const std::string & arg, const char * file, int line,
                           const char * stack) :
        GopherwoodIOException(arg, file, line, stack) {
    }

    ~GopherwoodRpcServerException() throw () {
    }

    const std::string & getErrClass() const {
        return errClass;
    }

    void setErrClass(const std::string & errClass) {
        this->errClass = errClass;
    }

    const std::string & getErrMsg() const {
        return errMsg;
    }

    void setErrMsg(const std::string & errMsg) {
        this->errMsg = errMsg;
    }

private:
    std::string errClass;
    std::string errMsg;
};

class GopherwoodTimeoutException: public GopherwoodIOException {
public:
    GopherwoodTimeoutException(const std::string & arg, const char * file, int line,
                         const char * stack) :
        GopherwoodIOException(arg, file, line, stack) {
    };

    ~GopherwoodTimeoutException() throw () {
    }
};

class GopherwoodIllegalStateException: public GopherwoodException {
public:
	GopherwoodIllegalStateException(const std::string & arg, const char * file, int line,
                         const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodIllegalStateException() throw () {
    }
};

class InvalidParameterException: public GopherwoodException {
public:
	InvalidParameterException(const std::string & arg, const char * file, int line,
                     const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~InvalidParameterException() throw () {
    }
};

class RpcNoSuchMethodException: public GopherwoodIOException {
public:
    RpcNoSuchMethodException(const std::string & arg, const char * file,
                             int line, const char * stack);

    ~RpcNoSuchMethodException() throw () {
    }
};

}

#endif /* _GOPHERWOOD_COMMON_EXCEPTION_H_ */
