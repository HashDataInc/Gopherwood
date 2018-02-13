/**
 * User: neuyilan@163.com
 * Date: 12/15/17
 * Time: 2:19 PM
 */

#include "QingStoreReadWrite.h"
#include "Logger.h"
#include "../common/Logger.h"
#include "FSConfig.h"

using namespace std;
namespace Gopherwood {
namespace Internal {
QingStoreReadWrite::QingStoreReadWrite() {

}

QingStoreReadWrite::~QingStoreReadWrite() {

}

int64_t QingStoreReadWrite::getCurrenttime() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

int64_t QingStoreReadWrite::qsWrite(char *filename, char *buffer, int32_t size) {
    LOG(INFO, "QingStoreReadWrite::qsWrite,  file name = %s", filename);
    int64_t writeLegnth = 0;
    if (putObject) {
        writeLegnth = ossWrite(qsContext, putObject, buffer, size);
        if (writeLegnth != size) {
            LOG(
                    LOG_ERROR,
                    "qingstor IN write failed with error message: %s, writeLegnth = %ld",
                    ossGetLastError(),
                    writeLegnth);
        }
    } else {
        LOG(LOG_ERROR, "qingstor OUT write failed with error message: %s", ossGetLastError());
    }
    return writeLegnth;
}

int64_t QingStoreReadWrite::qsRead(char *filename, char *buffer, int32_t size) {
//            LOG(INFO, "QingStoreReadWrite::qsRead, file name = %s", filename);
    int64_t readLegnth = 0;
    if (getObject) {
        readLegnth = ossRead(qsContext, getObject, buffer, size);
        LOG(INFO, "QingStoreReadWrite::qsRead,readLegnth = %ld", readLegnth);
        if (readLegnth != size) {
            LOG(
                    LOG_ERROR,
                    "qingstor IN  read failed with error message: %s,readLegnth = %ld",
                    ossGetLastError(),
                    readLegnth);
        }
    } else {
        LOG(LOG_ERROR, "qingstor OUT read failed with error message: %s", ossGetLastError());
    }
    return readLegnth;
}

void QingStoreReadWrite::getPutObject(char *filename) {
    putObject = ossPutObject(qsContext, bucket_name, filename, false);
}

void QingStoreReadWrite::getGetObject(char *filename) {
    getObject = ossGetObject(qsContext, bucket_name, filename, -1, -1);
}

void QingStoreReadWrite::closePutObject() {
    int res = ossCloseObject(qsContext, putObject);
    LOG(INFO, "QingStoreReadWrite::closePutObject, the res = %d", res);
}

void QingStoreReadWrite::closeGetObject() {
    int res = ossCloseObject(qsContext, getObject);
    LOG(INFO, "QingStoreReadWrite::closeGetObject, the res = %d", res);
}

void QingStoreReadWrite::initContext() {
    qsContext = ossInitContext(
            "QS",
            location,
            "",
            access_key_id,
            secret_access_key,
            write_buffer_size,
            read_buffer_size);
}

void QingStoreReadWrite::destroyContext() {
    ossDestroyContext(qsContext);
}

int QingStoreReadWrite::renameObject(char *beforeFilename, char *afterFilename) {
    int ret = ossMoveObject(qsContext, bucket_name, beforeFilename, bucket_name, afterFilename);
    LOG(
            INFO,
            "QingStoreReadWrite::renameObject, beforeFilename=%s, afterFilename=%s. the ret = %d",
            beforeFilename,
            afterFilename,
            ret);
    return 0;
}

int64_t QingStoreReadWrite::qsDeleteObject(char *filename) {
    int res = ossDeleteObject(qsContext, bucket_name, filename);
    LOG(INFO, "QingStoreReadWrite::qsDeleteObject, filename=%s, the res = %d", filename, res);
    return 0;
}

}
}
