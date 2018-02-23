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
#ifndef _GOPHERWOOD_CORE_GOPHERWOOD_H_
#define _GOPHERWOOD_CORE_GOPHERWOOD_H_

#include <fcntl.h> /* for O_RDONLY, O_WRONLY */
#include <errno.h> /* for EINTERNAL, etc. */
#include <stdint.h> /* for uint64_t, etc. */

#ifdef __cplusplus
extern "C" {
#endif

/*******************************************
 * All APIs set errno to meaningful values *
 *******************************************/
#ifndef EINTERNAL
#define EINTERNAL 255
#endif

/* error number mapping */
#define EGOPHERWOOD     100     //GopherException
#define ESYNC           101     //

/*******************************************
 * AccessFileType - the access file's type
 *******************************************/
#define	GW_RDONLY	0x0000		/* open for reading only */
#define	GW_WRONLY	0x0001		/* open for writing only */
#define	GW_RDWR		0x0002		/* open for reading and writing */
#define	GW_CREAT	0x0004		/* create if nonexistant */

#define GW_SEQ      0x0010      /* read or write in sequence mode */

typedef int32_t tSize; /// size of data for read/write io ops
typedef int64_t tOffset; /// offset within the file

struct GWFileSystemInternalWrapper;
typedef struct GWFileSystemInternalWrapper *gopherwoodFS;

struct GWFileInternalWrapper;
typedef struct GWFileInternalWrapper *gwFile;

/**
 * gwCreateContext - Connect to a gopherwood file system.
 *
 * @param fileName   the file name
 * @return Returns a handle to the filesystem or NULL on error.
 */
gopherwoodFS gwCreateContext(char *fileName);

/**
 * gwRead - Read data from an open file.
 *
 * @param   fs      The configured filesystem handle.
 * @param   file    The file handle.
 * @param   buffer  The buffer to copy read bytes into.
 * @param   length  The length of the buffer.
 * @return  On success, a positive number indicating how many bytes
 *          were read.
 *          On end-of-file, 0.
 *          On error, -1.  Errno will be set to the error code.
 *          Just like the POSIX read function, hdfsRead will return -1
 *          and set errno to EINTR if data is temporarily unavailable,
 *          but we are not yet at the end of the file.
 */
tSize gwRead(gopherwoodFS fs, gwFile file, void *buffer, tSize length);

/**
 * gwWrite - Write data into an open file.
 *
 * @param   fs      The configured filesystem handle.
 * @param   file    The file handle.
 * @param   buffer  The data.
 * @param   length  The no. of bytes to write.
 * @return  Returns the number of bytes written, -1 on error.
 */
tSize gwWrite(gopherwoodFS fs, gwFile file, const void *buffer, tSize length);

/**
 * gwFlush - Flush the data.
 *
 * @param     fs      The configured filesystem handle.
 * @param     file    The file handle.
 * @return    Returns 0 on success, -1 on error.
 */
int gwFlush(gopherwoodFS fs, gwFile file);

/**
 * gwOpenFile - Open a gopherwood file in given mode.
 *
 * @param   fs          The configured filesystem handle.
 * @param   fileName    The file name.
 * @param   flags       an | of bits/fcntl.h file flags -
 *          supported flags are O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT),
 *          O_WRONLY|O_APPEND and O_SYNC. Other flags are generally ignored other than (O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno equal ENOTSUP.
 * @param   bufferSize  Size of buffer for read/write - pass 0 if you want
 *          to use the default configured values.
 * @return  Returns the handle to the open file or NULL on error.
 */
gwFile gwOpenFile(gopherwoodFS fs, const char *fileName, int flags);

/**
 * gwSeek - Seek to given offset in file.
 * This works only for files opened in read-only mode.
 * @param file The file handle.
 * @param desiredPos Offset into the file to seek into.
 * @return Returns 0 on success, -1 on error.
 */
//TODO  in this implement, only the read-only mode can seek the file,
//      write mode are not allowed.
int gwSeek(gopherwoodFS fs, gwFile file, tOffset desiredPos);

/**
 * gwCloseFile - Close an open file.
 *
 * @param   fs      The configured filesystem handle.
 * @param   file    The file handle.
 * @return  Returns 0 on success, -1 on error.
 *          On error, errno will be set appropriately.
 *          If the alluxio file was valid, the memory associated with it will
 *          be freed at the end of this call, even if there was an I/O
 *          error.
 */
int gwCloseFile(gopherwoodFS fs, gwFile file);

/**
 * alluxioDelete - Delete file.
 *
 * @param   fs      The configured filesystem handle.
 * @param   path    The path of the file.
 * @param   recursive   if path is a directory and set to
 *          non-zero, the directory is deleted else throws an exception. In
 *          case of a file the recursive argument is irrelevant.
 * @return Returns 0 on success, -1 on error.
 */
int deleteFile(gopherwoodFS fs, gwFile file);

#ifdef __cplusplus
}
#endif

#endif /* _GOPHERWOOD_CORE_GOPHERWOOD_H_ */
