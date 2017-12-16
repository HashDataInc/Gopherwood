//
// Created by root on 11/17/17.
//



#include <sstream>
#include "FileSystemImpl.h"
#include "../common/Logger.h"
#include "../util/Coding.h"


namespace Gopherwood {
    namespace Internal {


        FileSystemImpl::FileSystemImpl(char *fileName) {
            SharedMemoryManager *smm = new SharedMemoryManager();
            std::shared_ptr<SharedMemoryManager> tmpsmm(smm);
            sharedMemoryManager = tmpsmm;

            LogFormat *lf = new LogFormat();
            std::shared_ptr<LogFormat> tmplf(lf);
            logFormat = tmplf;


            QingStoreReadWrite *qsrw = new QingStoreReadWrite();
            std::shared_ptr<QingStoreReadWrite> tmpqsrw(qsrw);
            qsReadWrite = tmpqsrw;

            sharedMemoryManager->checkSharedMemory();
            sharedMemoryManager->openSMBucket();
        }

        FileSystemImpl::~FileSystemImpl() {

        }

        //TODO
        int32_t FileSystemImpl::checkSSDFile() {

        }


        //TODO
        std::unordered_map<string, std::shared_ptr<FileStatus>>
        FileSystemImpl::rebuildFileStatusFromLog(char *fileName) {
            std::unordered_map<string, std::shared_ptr<FileStatus>> tmp;
            return tmp;
        };

        //TODO
        std::unordered_map<string, std::shared_ptr<FileStatus>>
        FileSystemImpl::catchUpFileStatusFromLog(int64_t logOffset) {
            std::unordered_map<string, std::shared_ptr<FileStatus>> tmp;
            return tmp;
        };

        bool FileSystemImpl::checkFileExist(char *fileName) {
            auto res = fileStatusMap.find(fileName);
            if (res != fileStatusMap.end()) {
                return true;
            }
            return false;
        }

        //TODO, needs some test
        void FileSystemImpl::createFile(char *fileName) {
            auto &status = fileStatusMap[fileName];
            if (!status) {
                status.reset(new FileStatus());
            }
            status->setFileName(fileName);

            string tmpFileName = fileStatusMap[fileName]->getFileName();
            LOG(INFO, "FileSystemImpl::createFile fileName = %s", tmpFileName.data());

//            acquireNewBlock(fileName);
        }

        std::shared_ptr<FileStatus> FileSystemImpl::getFileStatus(char *fileName) {
            return fileStatusMap[fileName];
        }

        void FileSystemImpl::acquireNewBlock(char *fileName) {
            //1. set the shared memory
            vector<int> blockIDVector = sharedMemoryManager->acquireNewBlock(fileName);

            assert(blockIDVector.size() == QUOTA_SIZE);

            for (int i = 0; i < blockIDVector.size(); i++) {
                LOG(INFO, "the acquired id block list index = %d, blockID = %d ", i, blockIDVector[i]);
            }

            auto &status = fileStatusMap[fileName];
            vector<int32_t> tmpVector = status->getBlockIdVector();
            tmpVector.insert(tmpVector.end(), blockIDVector.begin(), blockIDVector.end());


            //TODO, the above two line is the same as next line, however the code is wrong, why? @code status->getBlockIdVector().push_back(i);

            status->setBlockIdVector(tmpVector);

            if (blockIDVector.size() > 0) {
                status->setLastBucket(blockIDVector[blockIDVector.size() - 1]);
            }

            //2.  write the new file status to Log.
            string res = logFormat->serializeLog(blockIDVector, LogFormat::RecordType::acquireNewBlock);
            LOG(INFO, "8, LogFormat res size = %d", res.size());
            writeFileStatusToLog(fileName, res);
        }


        void FileSystemImpl::inactiveBlock(char *fileName, const std::vector<int32_t> &blockIdVector) {
            //1. set the shared memory
            for (int i = 0; i < blockIdVector.size(); i++) {
                sharedMemoryManager->inactiveBlock(blockIdVector[i], fileName);
            }
            //2.  write the new file status to Log.
            string res = logFormat->serializeLog(blockIdVector, LogFormat::RecordType::inactiveBlock);
            writeFileStatusToLog(fileName, res);

        }

        void FileSystemImpl::releaseBlock(char *fileName, const std::vector<int32_t> &blockIdVector) {
            //1. set the shared memory
            for (int i = 0; i < blockIdVector.size(); i++) {
                sharedMemoryManager->releaseBlock(blockIdVector[i]);
            }

            //2.  write the new file status to Log.
            string res = logFormat->serializeLog(blockIdVector, LogFormat::RecordType::releaseBlock);
            writeFileStatusToLog(fileName, res);
        }

        void FileSystemImpl::evictBlock(char *fileName, const std::vector<int32_t> &blockIdVector) {
            //1. set the shared memory
            for (int i = 0; i < blockIdVector.size(); i++) {
                sharedMemoryManager->evictBlock(blockIdVector[i]);
            }


            //2. evict data to OSS, like QingStor, S3.
            writeDate2OSS(fileName, blockIdVector);


            //3.  write the new file status to Log.
            string res = logFormat->serializeLog(blockIdVector, LogFormat::RecordType::evictBlock);
            writeFileStatusToLog(fileName, res);
        }


        std::string FileSystemImpl::constructFileKey(std::string str, int index) {
            stringstream ss;
            ss << str << "-" << index;
            return ss.str();
        }


        void FileSystemImpl::writeDate2OSS(char *fileName, const std::vector<int32_t> &blockIdVector) {
            qsReadWrite->initContext();

            for (int i = 0; i < blockIdVector.size(); i++) {
                int blockID = blockIdVector[i];
                int index = getIndexAccordingBlockID(fileName, blockID);


                string fileKey;
                fileKey.append(fileName, strlen(fileName));

                fileKey = constructFileKey(fileKey, index);

                LOG(INFO, "index = %d, fileKey = %s", index, fileKey.data());
                qsReadWrite->getPutObject((char *) fileKey.data());
                fsSeek(blockID * SIZE_OF_BLOCK, SEEK_SET);

                char *buffer = (char *) malloc(READ_BUFFER_SIZE * sizeof(char));
                const int iter = (int) (SIZE_OF_BLOCK / READ_BUFFER_SIZE);


                int64_t baseOffsetInBucket = blockID * SIZE_OF_BLOCK;
                int j;
                for (j = 0; j < iter; j++) {
                    int32_t readBytes = readDataFromBucket(buffer, READ_BUFFER_SIZE);
                    LOG(INFO, "readBytes = %d", readBytes);
                    if (readBytes <= 0) {
                        break;
                    }
                    qsReadWrite->qsWrite((char *) fileKey.data(), buffer, readBytes);
                    baseOffsetInBucket += READ_BUFFER_SIZE;
                    fsSeek(baseOffsetInBucket, SEEK_SET);
                }
                qsReadWrite->closePutObject();
            }
            qsReadWrite->destroyContext();
        }

        int FileSystemImpl::getIndexAccordingBlockID(char *fileName, int blockID) {
            std::shared_ptr<FileStatus> fileStatus = fileStatusMap[fileName];;
            for (int i = 0; i < fileStatus->getBlockIdVector().size(); i++) {
                if (fileStatus->getBlockIdVector()[i] == blockID) {
                    return i;
                }
            }
        }


        int64_t FileSystemImpl::getTheEOFOffset(const char *fileName) {
            vector<int32_t> blockIDVector = fileStatusMap[fileName]->getBlockIdVector();


            int32_t lastBucket = fileStatusMap[fileName]->getLastBucket();
            int64_t endOffsetOfBucket = fileStatusMap[fileName]->getEndOffsetOfBucket();

            int64_t realEOFOffset = 0;

            for (int i = 0; i < blockIDVector.size(); i++) {
                if (blockIDVector[i] == lastBucket) {
                    break;
                }
                realEOFOffset += SIZE_OF_BLOCK;
            }

            realEOFOffset += endOffsetOfBucket;
            return realEOFOffset;
        }


        std::shared_ptr<FileStatus> FileSystemImpl::getFileStatus(const char *fileName) {
            return fileStatusMap[fileName];
        }


        int64_t FileSystemImpl::readDataFromBucket(char *buf, int32_t size) {
            checkBucketFileOpen();
            int64_t length = read(bucketFd, buf, size);
            return length;
        }


        void FileSystemImpl::writeDataToBucket(char *buf, int64_t size) {
            checkBucketFileOpen();
            int64_t res = write(bucketFd, buf, size);
            if (res == -1) {
                LOG(INFO, "some error occur, can not write to the ssd file");
            }
        }


        void FileSystemImpl::checkBucketFileOpen() {
            int flags = O_CREAT | O_RDWR;
            if (bucketFd == -1) {
                LOG(INFO, "the ssd file do not open, so open it");
                bucketFd = open(BUCKET_PATH_FILE_NAME, flags, 0644);
            }
        }

        int32_t FileSystemImpl::fsSeek(int64_t offset, int whence) {
            checkBucketFileOpen();
            int64_t res = lseek(bucketFd, offset, whence);
            if (res == -1) {
                LOG(INFO, "fsSeek error");
            }
        }

//
        void FileSystemImpl::closeBucketFile() {
            close(bucketFd);
        }

        void FileSystemImpl::stopSystem() {
            closeBucketFile();
            sharedMemoryManager->closeSMFile();
            sharedMemoryManager->closeSMBucket();
        }

        //TODO, flush, write log and so on
        void FileSystemImpl::closeFile(char *fileName) {
            LOG(INFO, "come in the closeFile");
            persistentFileLog(fileName);

        }

        void FileSystemImpl::persistentFileLog(char *fileName) {
            int flags = O_CREAT | O_RDWR | O_TRUNC;
            char *filePathName = getFilePath(fileName);
            LOG(INFO, "filePathName = %s", filePathName);
            int logFd = open(filePathName, flags, 0644);

            std::shared_ptr<FileStatus> status = fileStatusMap[fileName];

            char *tmpRes = status->serializeFileStatus();
            int tmpTotalLength = *((int *) tmpRes);


            char buf[tmpTotalLength + 4];
            memcpy(buf, tmpRes, tmpTotalLength + 4);


            LOG(INFO, "FileSystemImpl::persistentFileLog buf size= %d", tmpTotalLength + 4);

            int32_t length = write(logFd, buf, tmpTotalLength + 4);

            close(logFd);

            LOG(INFO, "FileSystemImpl::persistentFileLog write size = %d", length);
        }

        char *FileSystemImpl::getFilePath(char *fileName) {
            char *filePathName = static_cast<char *>(malloc(strlen(fileName) + strlen(FILE_LOG_PERSISTENCE_PATH) + 1));
            strcpy(filePathName, FILE_LOG_PERSISTENCE_PATH);
            strcat(filePathName, fileName);
            return filePathName;
        }


        void FileSystemImpl::writeFileStatusToLog(char *fileName, std::string dataStr) {
            int flags = O_CREAT | O_RDWR | O_APPEND;

            char *filePathName = getFilePath(fileName);
            LOG(INFO, "filePathName = %s", filePathName);

            int logFd = open(filePathName, flags, 0644);
            LOG(INFO, "9, LogFormat dataStr size = %d", dataStr.size());
            int32_t length = write(logFd, dataStr.data(), dataStr.size());

            close(logFd);

            LOG(INFO, "FileSystemImpl::writeFileStatusToLog write size = %d", length);

            readFileStatusFromLog(fileName);
        }


        void FileSystemImpl::readFileStatusFromLog(char *fileName) {
            int flags = O_CREAT | O_RDWR;

            char *filePathName = getFilePath(fileName);

            int logFd = open(filePathName, flags, 0644);


            char bufLength[4];
            int32_t length = read(logFd, bufLength, sizeof(bufLength));

            while (length == 4) {
                LOG(INFO, "FileSystemImpl::readFileStatusFromLog read length = %d", length);
                int32_t dataSize = DecodeFixed32(bufLength);
                std::string logRecord;
                char res[dataSize];
                read(logFd, res, sizeof(res));
                logRecord.append(res, dataSize);
                logFormat->deserializeLog(logRecord);

                length = read(logFd, bufLength, sizeof(bufLength));
            }
            LOG(INFO, "FileSystemImpl::readFileStatusFromLog out read length = %d", length);

            close(logFd);


//
//            LOG(INFO, "FileSystemImpl::readFileStatusFromLog first read length  = %d", length);
//            LOG(INFO, "FileSystemImpl::readFileStatusFromLog dataSize  = %d", dataSize);
//
//
//            length = read(logFd, res, sizeof(res));
//            LOG(INFO, "FileSystemImpl::readFileStatusFromLog second read length  = %d", length);
//
//            char type = *res;
//            LOG(INFO, "FileSystemImpl::readFileStatusFromLog type   = %d", type);
//
//            int offset = 1;
//
//            int32_t numOfBlocks = DecodeFixed32(res + offset);
//            offset += 4;
//            LOG(INFO, "FileSystemImpl::readFileStatusFromLog numOfBlocks   = %d", numOfBlocks);
//
//            for (int i = 0; i < numOfBlocks; i++) {
//                int32_t blockID = DecodeFixed32(res + offset);
//                LOG(INFO, "FileSystemImpl::readFileStatusFromLog numOfBlocks   = %d", blockID);
//                offset += 4;
//            }
//
//            int pid = DecodeFixed32(res + offset);
//            LOG(INFO, "FileSystemImpl::readFileStatusFromLog pid   = %d", pid);
//            close(logFd);

        }


    }

}