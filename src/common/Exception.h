/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
/********************************************************************
 * 2014 -
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

class GopherwoodIOException: public GopherwoodException {
public:
    GopherwoodIOException(const std::string & arg, const char * file, int line,
                    const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodIOException() throw () {
    }

public:
    static const char * ReflexName;
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

class AccessControlException: public GopherwoodException {
public:
    AccessControlException(const std::string & arg, const char * file, int line,
                           const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~AccessControlException() throw () {
    }

public:
    static const char * ReflexName;
};

class AlreadyBeingCreatedException: public GopherwoodException {
public:
    AlreadyBeingCreatedException(const std::string & arg, const char * file,
                                 int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~AlreadyBeingCreatedException() throw () {
    }

public:
    static const char * ReflexName;
};

class ChecksumException: public GopherwoodException {
public:
    ChecksumException(const std::string & arg, const char * file, int line,
                      const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~ChecksumException() throw () {
    }
};

class DSQuotaExceededException: public GopherwoodException {
public:
    DSQuotaExceededException(const std::string & arg, const char * file,
                             int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~DSQuotaExceededException() throw () {
    }

public:
    static const char * ReflexName;
};

class FileAlreadyExistsException: public GopherwoodException {
public:
    FileAlreadyExistsException(const std::string & arg, const char * file,
                               int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~FileAlreadyExistsException() throw () {
    }

public:
    static const char * ReflexName;
};

class FileNotFoundException: public GopherwoodException {
public:
    FileNotFoundException(const std::string & arg, const char * file, int line,
                          const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~FileNotFoundException() throw () {
    }

public:
    static const char * ReflexName;
};

class GopherwoodBadBoolFoumat: public GopherwoodException {
public:
    GopherwoodBadBoolFoumat(const std::string & arg, const char * file, int line,
                      const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodBadBoolFoumat() throw () {
    }
};

class GopherwoodBadConfigFoumat: public GopherwoodException {
public:
    GopherwoodBadConfigFoumat(const std::string & arg, const char * file, int line,
                        const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodBadConfigFoumat() throw () {
    }
};

class GopherwoodBadNumFoumat: public GopherwoodException {
public:
    GopherwoodBadNumFoumat(const std::string & arg, const char * file, int line,
                     const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodBadNumFoumat() throw () {
    }
};

class GopherwoodCanceled: public GopherwoodException {
public:
    GopherwoodCanceled(const std::string & arg, const char * file, int line,
                 const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodCanceled() throw () {
    }
};

class GopherwoodFileSystemClosed: public GopherwoodException {
public:
    GopherwoodFileSystemClosed(const std::string & arg, const char * file, int line,
                         const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodFileSystemClosed() throw () {
    }
};

class GopherwoodConfigInvalid: public GopherwoodException {
public:
    GopherwoodConfigInvalid(const std::string & arg, const char * file, int line,
                      const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodConfigInvalid() throw () {
    }
};

class GopherwoodConfigNotFound: public GopherwoodException {
public:
    GopherwoodConfigNotFound(const std::string & arg, const char * file, int line,
                       const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodConfigNotFound() throw () {
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

class GopherwoodInvalidBlockToken: public GopherwoodException {
public:
    GopherwoodInvalidBlockToken(const std::string & arg, const char * file, int line,
                          const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodInvalidBlockToken() throw () {
    }

public:
    static const char * ReflexName;
};

/**
 * This will wrap GopherwoodNetworkConnectionException and GopherwoodTimeoutException.
 * This exception will be caught and attempt will be performed to recover in HA case.
 */
class GopherwoodFailoverException: public GopherwoodException {
public:
    GopherwoodFailoverException(const std::string & arg, const char * file, int line,
                          const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodFailoverException() throw () {
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

class GopherwoodTimeoutException: public GopherwoodException {
public:
    GopherwoodTimeoutException(const std::string & arg, const char * file, int line,
                         const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~GopherwoodTimeoutException() throw () {
    }
};

class InvalidParameter: public GopherwoodException {
public:
    InvalidParameter(const std::string & arg, const char * file, int line,
                     const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~InvalidParameter() throw () {
    }

public:
    static const char * ReflexName;
};

class HadoopIllegalArgumentException : public InvalidParameter {
public:
    HadoopIllegalArgumentException(const std::string& arg, const char* file,
                                   int line, const char* stack)
        : InvalidParameter(arg, file, line, stack) {
    }

    ~HadoopIllegalArgumentException() throw() {
    }

public:
    static const char* ReflexName;
};

class InvalidPath: public GopherwoodException {
public:
    InvalidPath(const std::string & arg, const char * file, int line,
                const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~InvalidPath() throw () {
    }
};

class NotReplicatedYetException: public GopherwoodException {
public:
    NotReplicatedYetException(const std::string & arg, const char * file,
                              int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~NotReplicatedYetException() throw () {
    }

public:
    static const char * ReflexName;
};

class NSQuotaExceededException: public GopherwoodException {
public:
    NSQuotaExceededException(const std::string & arg, const char * file,
                             int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~NSQuotaExceededException() throw () {
    }

public:
    static const char * ReflexName;
};

class ParentNotDirectoryException: public GopherwoodException {
public:
    ParentNotDirectoryException(const std::string & arg, const char * file,
                                int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~ParentNotDirectoryException() throw () {
    }

public:
    static const char * ReflexName;
};

class ReplicaNotFoundException: public GopherwoodException {
public:
    ReplicaNotFoundException(const std::string & arg, const char * file,
                             int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~ReplicaNotFoundException() throw () {
    }

public:
    static const char * ReflexName;
};

class SafeModeException: public GopherwoodException {
public:
    SafeModeException(const std::string & arg, const char * file, int line,
                      const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~SafeModeException() throw () {
    }

public:
    static const char * ReflexName;
};

class UnresolvedLinkException: public GopherwoodException {
public:
    UnresolvedLinkException(const std::string & arg, const char * file,
                            int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~UnresolvedLinkException() throw () {
    }

public:
    static const char * ReflexName;
};

class UnsupportedOperationException: public GopherwoodException {
public:
    UnsupportedOperationException(const std::string & arg, const char * file,
                                  int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~UnsupportedOperationException() throw () {
    }

public:
    static const char * ReflexName;
};

class SaslException: public GopherwoodException {
public:
    SaslException(const std::string & arg, const char * file, int line,
                  const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~SaslException() throw () {
    }

public:
    static const char * ReflexName;
};

class NameNodeStandbyException: public GopherwoodException {
public:
    NameNodeStandbyException(const std::string & arg, const char * file,
                             int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~NameNodeStandbyException() throw () {
    }

public:
    static const char * ReflexName;
};

class RpcNoSuchMethodException: public GopherwoodException {
public:
    RpcNoSuchMethodException(const std::string & arg, const char * file,
                             int line, const char * stack) :
        GopherwoodException(arg, file, line, stack) {
    }

    ~RpcNoSuchMethodException() throw () {
    }

public:
    static const char * ReflexName;
};

class RecoveryInProgressException : public GopherwoodException {
 public:
  RecoveryInProgressException(const std::string & arg, const char * file,
                              int line, const char * stack)
      : GopherwoodException(arg, file, line, stack) {
  }

  ~RecoveryInProgressException() throw () {
  }

 public:
  static const char * ReflexName;
};

}

#endif /* _GOPHERWOOD_COMMON_EXCEPTION_H_ */
