
#ifndef _GOPHERWOOD_CORE_GOPHERWOOD_H_
#define _GOPHERWOOD_CORE_GOPHERWOOD_H_



//#include "FileSystem.h"
//#include "InputStream.h"
//#include "OutputStream.h"
//#include "Exception.h"
//#include "Logger.h"
//#include "XmlConfig.h"
//#include "fcntl.h"

//the access file's type

/** All APIs set errno to meaningful values */



typedef int32_t tSize; /// size of data for read/write io ops
typedef int64_t tOffset; /// offset within the file


struct GWFileSystemInternalWrapper;
typedef struct GWFileSystemInternalWrapper *gopherwoodFS;

struct GWFileInternalWrapper;
typedef struct GWFileInternalWrapper *gwFile;


typedef enum AccessFileType {

    sequenceType = 0,

    randomType = 1,

    hintRandomType = 2,

    hintSequenceType = 3,
};


typedef struct FileInfo {

    tOffset fileSize;

} GWFileInfo;


/**
 * gwCreateContext - Connect to a gopherwood file system.
 * @param fileName   the file name
 */
gopherwoodFS gwCreateContext();


int destroyContext(gopherwoodFS fs);


/**
 * gwRead - Read data from an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param buffer The buffer to copy read bytes into.
 * @param length The length of the buffer.
 * @return      On success, a positive number indicating how many bytes
 *              were read.
 *              On end-of-file, 0.
 *              On error, -1.  Errno will be set to the error code.
 *              Just like the POSIX read function, hdfsRead will return -1
 *              and set errno to EINTR if data is temporarily unavailable,
 *              but we are not yet at the end of the file.
 */
tSize gwRead(gopherwoodFS fs, gwFile file, void *buffer, tSize length);


/**
 * gwWrite - Write data into an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param buffer The data.
 * @param length The no. of bytes to write.
 * @return Returns the number of bytes written, -1 on error.
 */
tSize gwWrite(gopherwoodFS fs, gwFile file, const void *buffer, tSize length);


///**
// * gwFlush - Flush the data.
// * @param fs The configured filesystem handle.
// * @param file The file handle.
// * @return Returns 0 on success, -1 on error.
// */
//int gwFlush(gopherwoodFS fs, gwFile file);


/**
 * gwOpenFile - Open a gopherwood file in given mode.
 * @param fs The configured filesystem handle.
 * @param fileName The file name.
 * @param flags - an | of bits/fcntl.h file flags - supported flags are O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT),
 * O_WRONLY|O_APPEND and O_SYNC. Other flags are generally ignored other than (O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno equal ENOTSUP.
 * @param bufferSize Size of buffer for read/write - pass 0 if you want
 * to use the default configured values.
 * @return Returns the handle to the open file or NULL on error.
 */
gwFile gwOpenFile(gopherwoodFS fs, const char *fileName, int flags);



/**
 * gwSeek - Seek to given offset in file.
 * This works only for files opened in read-only mode.
 * @param file The file handle.
 * @param desiredPos Offset into the file to seek into.
 * @return Returns 0 on success, -1 on error.
 */
//TODO in this implement, only the read-only mode can seek the file,
//TODO write mode are not allowed.
int gwSeek(gopherwoodFS fs, gwFile file, tOffset desiredPos,int where);


int gwCloseFile(gopherwoodFS fs, gwFile file);


//int deleteFile(gopherwoodFS fs, gwFile file);
//TODO
int deleteFile(char *filePath);


//TODO
int cancelFile(gopherwoodFS fs, gwFile file);


GWFileInfo *gwStatFile(gopherwoodFS fs, gwFile file);


#endif /* _GOPHERWOOD_CORE_GOPHERWOOD_H_ */