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

        void QingStoreReadWrite::qsRead(char *filename) {

        }


        int64_t QingStoreReadWrite::getCurrenttime() {
            struct timeval tv;
            gettimeofday(&tv, NULL);
            return tv.tv_sec * 1000 + tv.tv_usec / 1000;
        }

        static double calculateThroughput(int64_t elapsed, int64_t size) {
            return size / 1024.0 * 1000.0 / 1024.0 / elapsed;
        }

        void QingStoreReadWrite::qsWrite(char *filename, char *buffer, int32_t size) {
            LOG(INFO, "testPutObject  file name = %s", filename);
            if (putObject) {

                if (qingstorWrite(qsContext, putObject, buffer, size) != size) {
                    LOG(LOG_ERROR, "qingstor write failed with error message: %s", qingstorGetLastError());
                    return;
                }

            } else {
                LOG(LOG_ERROR, "qingstor write failed with error message: %s", qingstorGetLastError());
            }
            return;
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