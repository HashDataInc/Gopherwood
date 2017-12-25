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

        static double calculateThroughput(int64_t elapsed, int64_t size) {
            return size / 1024.0 * 1000.0 / 1024.0 / elapsed;
        }

        int64_t QingStoreReadWrite::qsWrite(char *filename, char *buffer, int32_t size) {
            LOG(INFO, "QingStoreReadWrite::qsWrite,  file name = %s", filename);
            int64_t writeLegnth = 0;
            if (putObject) {
                writeLegnth = qingstorWrite(qsContext, putObject, buffer, size);
                if (writeLegnth != size) {
                    LOG(LOG_ERROR, "qingstor IN write failed with error message: %s, writeLegnth = %d",
                        qingstorGetLastError(), writeLegnth);
                }
            } else {
                LOG(LOG_ERROR, "qingstor OUT write failed with error message: %s", qingstorGetLastError());
            }
            return writeLegnth;
        }


        int64_t QingStoreReadWrite::qsRead(char *filename, char *buffer, int32_t size) {
//            LOG(INFO, "QingStoreReadWrite::qsRead, file name = %s", filename);
            int64_t readLegnth = 0;
            if (getObject) {
                readLegnth = qingstorRead(qsContext, getObject, buffer, size);
                if (readLegnth != size) {
                    LOG(LOG_ERROR, "qingstor IN  read failed with error message: %s,readLegnth = %d",
                        qingstorGetLastError(), readLegnth);
                }
            } else {
                LOG(LOG_ERROR, "qingstor OUT read failed with error message: %s", qingstorGetLastError());
            }
            return readLegnth;
        }

        //  TODO .the file is logcal deleted
        int64_t QingStoreReadWrite::qsDeleteObject(char *filename) {
            LOG(INFO, "QingStoreReadWrite::qsDeleteObject. delete file with name = %s", filename);
            int deleteLength = qingstorDeleteObject(qsContext, bucket_name, filename);
            LOG(INFO, "QingStoreReadWrite::qsDeleteObject. delete length = %d", deleteLength);
            return 0;
        }


        void QingStoreReadWrite::getPutObject(char *filename) {
            putObject = qingstorPutObject(qsContext, bucket_name, filename);
        }

        void QingStoreReadWrite::getGetObject(char *filename) {
            getObject = qingstorGetObject(qsContext, bucket_name, filename, -1, -1);
        }

        void QingStoreReadWrite::closePutObject() {
            qingstorCloseObject(qsContext, putObject);
        }

        void QingStoreReadWrite::closeGetObject() {
            qingstorCloseObject(qsContext, getObject);
        }


        void QingStoreReadWrite::initContext() {
            qsContext = qingstorInitContext(location, access_key_id, secret_access_key, QINGSTOR_BUFFER_SIZE);
        }

        void QingStoreReadWrite::destroyContext() {
            qingstorDestroyContext(qsContext);
        }


    }
}