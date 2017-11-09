#ifndef SSDKVCLIENT_H_INCLUDED
#define SSDKVCLIENT_H_INCLUDED



#endif // SSDKVCLIENT_H_INCLUDED

typedef int32_t tSize; /// size of data for read/write io ops
typedef int64_t tOffset; /// offset within the file

struct SSDKVFileInternalWrapper;
typedef struct SSDKVFileInternalWrapper * ssdkvFile;

/**
 * ssdkvDeleteFile - Delete file.
 * @param fs The configured filesystem handle.
 * @return Returns 0 on success, -1 on error.
 */
int ssdkvDeleteFile(ssdkvFile fs);

/**
 * ssdkvOpenFile - Open a ssdkvFile according to the fileName
 * @param fileName The name of the file to be opened
 * @return Returns the handle to the open file or NULL on error.
 */
ssdkvFile ssdkvOpenFile(const char * fileName);

/**
 * ssdkvCloseFile - Close an open file.
 * @param fileName The name of the file to be closed
 * @return Returns 0 on success, -1 on error.
 */
int ssdkvCloseFile(const char * fileName);




/**
 * ssdkvRead - Read data from an open file.
 * @param file The file handle.
 * @param buffer The buffer to copy read bytes into.
 * @param length The length of the buffer.
 * @return      On success, a positive number indicating how many bytes
 *              were read.
 *              On end-of-file, 0.
 *              On error, -1.
 */
tSize ssdkvRead(ssdkvFile file, void * buffer, tSize length);


/**
 * ssdkvWrite - Write data into an open file.
 * @param file The file handle.
 * @param buffer The data.
 * @param length The no. of bytes to write.
 * @return Returns the number of bytes written, -1 on error.
 */
tSize ssdkvWrite(ssdkvFile file, const void * buffer, tSize length);


/**
 * ssdkvSeek - Seek to given offset in file.
 * @param file The file handle.
 * @param desiredPos Offset into the file to seek into.
 * @return Returns 0 on success, -1 on error.
 */
int ssdkvSeek(ssdkvFile file, tOffset desiredPos);


/**
 * ssdkvFlush - flush the data of the file to persistent storage.
 * @param file The file handle.
 * @return Returns 0 on success, -1 on error.
 */
int ssdkvFlush(ssdkvFile file);


