#include <sstream>
#include "FileSystemImpl.h"
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
            qsReadWrite->initContext();

            sharedMemoryManager->checkSharedMemory();
            sharedMemoryManager->openSMBucket();
        }

        FileSystemImpl::~FileSystemImpl() {

        }

        //TODO
        int32_t FileSystemImpl::checkSSDFile() {

        }


        void FileSystemImpl::changePingBlockActive(int blockID) {
            sharedMemoryManager->activeBlock(blockID);
        }


        //TODO ******************************
        void FileSystemImpl::writeDataFromOSS2Bucket(int64_t ossindex, string fileName) {
            //1. prapre qingstor context
            string ossFileName = constructFileKey(fileName, ossindex + 1);
            qsReadWrite->getGetObject((char *) ossFileName.data());

            // 1. get the block which can write and seek to the begin of the block.
            int blockID = getOneBlockForWrite(ossindex, fileName);
            auto &status = fileStatusMap[fileName];

            int64_t totalOffset = blockID * SIZE_OF_BLOCK;

            fsSeek(totalOffset, SEEK_SET);

            LOG(INFO, "FileSystemImpl::writeDataFromOSS2Bucket. blockID=%d,", blockID);
            //2. read data from OSS, and write the data to bucket.
            char buf[SIZE_OF_BLOCK / 8];
            int64_t readLength = qsReadWrite->qsRead((char *) ossFileName.data(), buf, sizeof(buf));
            while (readLength > 0) {
                writeDataToBucket(buf, readLength);
                totalOffset += readLength;
                fsSeek(totalOffset, SEEK_SET);

                readLength = qsReadWrite->qsRead((char *) ossFileName.data(), buf, sizeof(buf));

//                LOG(INFO, "FileSystemImpl::writeDataFromOSS2Bucket. readLength=%d,", readLength);
            }

            qsReadWrite->closeGetObject();

            //3. delete the data in oss.
            qsReadWrite->qsDeleteObject(ossFileName.c_str());

            //4. write replace log, delete it because it have been writen to log in the acquire block logs.
            // we use this block to replace the one block in OSS

            // use bloclID to replace -(ossindex + 1)
            std::vector<int32_t> deleteBlockVector;
            deleteBlockVector.push_back(blockID);
            deleteBlockVector.push_back(-(ossindex + 1));
            string res = logFormat->serializeLog(deleteBlockVector, LogFormat::RecordType::deleteBlock);
            writeFileStatusToLog((char *) fileName.data(), res);
        }

        /**
         * get one block from ping block lists.
         * @param fileName
         * @return
         */
        int FileSystemImpl::getOneBlockForWrite(int ossindex, string fileName) {
            auto &status = fileStatusMap[fileName];
            int blockID = status->getLastBucket();
            LOG(INFO, "FileSystemImpl::getOneBlockForWrite, the last blockID=%d", blockID);

            // 1. get one block which type='0', this is the last bucket's next block
            int index = getIndexAccordingBlockID((char *) fileName.data(), blockID) + 1;
            LOG(INFO, "FileSystemImpl::getOneBlockForWrite, before index  = %d,status->getBlockIdVector().size()=%d",
                index, status->getBlockIdVector().size());

            while (index >= status->getBlockIdVector().size()) {
                acquireNewBlock((char *) fileName.data());
                index = getIndexAccordingBlockID((char *) fileName.data(), blockID) + 1;
                LOG(LOG_ERROR,
                    "FileSystemImpl::getOneBlockForWrite, ACQUIRE ZERO BLOCKS, SO ACQUIRE AGAIN UNTIL GET ONE");
            }
            LOG(INFO, "FileSystemImpl::getOneBlockForWrite, after index  = %d, status->getBlockIdVector().size()=%d",
                index, status->getBlockIdVector().size());


            vector<int32_t> blockIDVector = status->getBlockIdVector();
            int newBlockID = blockIDVector[index];
            /******************TODO FOR TEST****************************/
            for (int i = 0; i < blockIDVector.size(); i++) {
                LOG(INFO, "FileSystemImpl::getOneBlockForWrite, before blockIDVector[i] = %d,", blockIDVector[i]);
            }

            for (int i = 0; i < status->getPingIDVector().size(); i++) {
                LOG(INFO, "FileSystemImpl::getOneBlockForWrite, before ping blockIDVector[i] = %d,",
                    status->getPingIDVector()[i]);
            }

            /******************TODO FOR TEST****************************/



            //2. check whether the negative blockID is the last block ID
            int tmpBlockID = -(ossindex + 1);
            if (status->getLastBucket() == tmpBlockID) {
                status->setLastBucket(newBlockID);
                LOG(INFO,
                    "FileSystemImpl::getOneBlockForWrite. the negative blockID is the last block ID, so replace it");
            }


            //3. use the newBlockID to replace the negative block id in OSS
            blockIDVector[ossindex] = newBlockID;


            //4.delete it from block id vector
            blockIDVector.erase(blockIDVector.begin() + index);
            status->setBlockIdVector(blockIDVector);


            /******************TODO FOR TEST****************************/
            for (int i = 0; i < blockIDVector.size(); i++) {
                LOG(INFO, "FileSystemImpl::getOneBlockForWrite, after blockIDVector[i] = %d,", blockIDVector[i]);
            }

            for (int i = 0; i < status->getPingIDVector().size(); i++) {
                LOG(INFO, "FileSystemImpl::getOneBlockForWrite, after ping blockIDVector[i] = %d,",
                    status->getPingIDVector()[i]);
            }
            /******************TODO FOR TEST****************************/


            return newBlockID;
        }


        /**
         * check whether the blockID belongs to the file or not,
         */
        bool FileSystemImpl::checkBlockIDWithFileName(int blockID, string fileName) {
            string smFileName = sharedMemoryManager->getFileNameAccordingBlockID(blockID);
            LOG(INFO, "FileSystemImpl::checkBlockIDWithFileName. smFileName=%s, fileName=%s", smFileName.data(),
                fileName.data());
            if (std::strcmp(fileName.data(), smFileName.data()) == 0) {
                LOG(INFO, "FileSystemImpl::checkBlockIDWithFileName. fileName == smFileName");
                return true;
            }
            LOG(INFO, "FileSystemImpl::checkBlockIDWithFileName. fileName != smFileName");
            return false;
        }

        bool FileSystemImpl::checkBlockIDWithFileNameAndType(int blockID, string fileName) {
            string smFileName = sharedMemoryManager->getFileNameAccordingBlockID(blockID);
            char type = sharedMemoryManager->getBlockType(blockID);

            LOG(INFO, "FileSystemImpl::checkBlockIDWithFileNameAndType. smFileName=%s, fileName=%s", smFileName.data(),
                fileName.data());
            if ((std::strcmp(fileName.data(), smFileName.data()) == 0) && (type == '2')) {
                LOG(INFO, "FileSystemImpl::checkBlockIDWithFileNameAndType. fileName == smFileName");
                return true;
            }
            LOG(INFO, "FileSystemImpl::checkBlockIDWithFileNameAndType. fileName != smFileName");
            return false;
        }


        //TODO
        void FileSystemImpl::rebuildFileStatusFromLog(char *fileName) {
            LOG(INFO, "FileSystemImpl::rebuildFileStatusFromLog, come in the rebuildFileStatusFromLog file");
            auto &fileStatus = fileStatusMap[fileName];
            if (!fileStatus) {
                fileStatus.reset(new FileStatus());
            }

            int flags = O_CREAT | O_RDWR;

            char *filePathName = getFilePath(fileName);
            int logFd = open(filePathName, flags, 0644);

            //1. rebuild file status from log
            char bufLength[4];
            int32_t length = read(logFd, bufLength, sizeof(bufLength));
            while (length == 4) {
                int32_t dataSize = DecodeFixed32(bufLength);
//                LOG(INFO, "FileSystemImpl::readCloseFileStatus read length = %d, dataSize size =%d", length, dataSize);
                std::string logRecord;
                char res[dataSize];
                read(logFd, res, sizeof(res));
                logRecord.append(res, dataSize);
                logFormat->deserializeLog(logRecord, fileStatus);
                length = read(logFd, bufLength, sizeof(bufLength));
            }
            close(logFd);


            std::vector<int32_t> pingBlockVector;
            int quotaCount = 0;

            int blockIDSize = fileStatus->getBlockIdVector().size();

            if (blockIDSize > 0) {
                //2. set last bucket
                int lastBlockID = fileStatus->getBlockIdVector()[blockIDSize - 1];

                //3. BUGFIX, check the rebuild file status is right?. because when other file rebuild from file status,
                // it will check the status is right or not. maybe the other file have change the its status while have not write log
                if (lastBlockID >= 0) {
                    bool isEqual = checkBlockIDWithFileNameAndType(lastBlockID, fileName);

                    while ((!isEqual) && (lastBlockID >= 0)) {
                        LOG(INFO, "FileSystemImpl::rebuildFileStatusFromLog NOT EQUAL");
                        int logFd = open(filePathName, flags, 0644);
                        fileStatus.reset(new FileStatus());
                        char bufLength[4];
                        int32_t length = read(logFd, bufLength, sizeof(bufLength));
                        while (length == 4) {
                            int32_t dataSize = DecodeFixed32(bufLength);
//                LOG(INFO, "FileSystemImpl::readCloseFileStatus read length = %d, dataSize size =%d", length, dataSize);
                            std::string logRecord;
                            char res[dataSize];
                            read(logFd, res, sizeof(res));
                            logRecord.append(res, dataSize);
                            logFormat->deserializeLog(logRecord, fileStatus);
                            length = read(logFd, bufLength, sizeof(bufLength));
                        }

                        lastBlockID = fileStatus->getBlockIdVector()[blockIDSize - 1];
                        if (lastBlockID >= 0) {
                            isEqual = checkBlockIDWithFileNameAndType(lastBlockID, fileName);
                        }
                        close(logFd);
                    }
                }


                //4. set the last bucket
                fileStatus->setLastBucket(lastBlockID);

                //5. add the last bucket to ping block if its not in the OSS
                if (lastBlockID >= 0) {

                    //5.1 change the shared memory
                    changePingBlockActive(lastBlockID);
                    //5.2 add to the ping block vector
                    pingBlockVector.push_back(lastBlockID);
                    quotaCount++;
                }

            }

            //3. set ping block vector
            int blockIndex = 0;
            while ((blockIndex < blockIDSize) && (quotaCount < QUOTA_SIZE)) {
                int tmpBlockID = fileStatus->getBlockIdVector()[blockIndex];
                int lastBlockID = fileStatus->getLastBucket();
                if ((tmpBlockID >= 0) && (tmpBlockID != lastBlockID) &&
                    (checkBlockIDWithFileNameAndType(tmpBlockID, fileName))) {
                    //3.1 change the shared memory
                    changePingBlockActive(tmpBlockID);
                    //3.2 add to the ping block vector
                    pingBlockVector.push_back(tmpBlockID);
                    quotaCount++;
                }
                blockIndex++;
            }
            fileStatus->setPingIDVector(pingBlockVector);

            //4. change the shared memory
            for (int i = 0; i < pingBlockVector.size(); i++) {
                int pingID = pingBlockVector[i];
                changePingBlockActive(pingID);
            }

            LOG(INFO, "******* rebuildFileStatusFromLog in the end, before the close File Status***********");
            int64_t endOffsetOfBucket = fileStatus->getEndOffsetOfBucket();
            vector<int32_t> blockIDVector = fileStatus->getBlockIdVector();
            LOG(INFO, "endOffsetOfBucket=%d,blockIDVector.size=  %d", endOffsetOfBucket, blockIDVector.size());
            for (int i = 0; i < blockIDVector.size(); i++) {
                LOG(INFO, " block id = %d", blockIDVector[i]);
            }

            LOG(INFO, "****** rebuildFileStatusFromLog in the end, after the close File Status*********");


        }

        //TODO
        void FileSystemImpl::catchUpFileStatusFromLog(char *fileName, int64_t logOffset) {

            //1. open file
            int flags = O_CREAT | O_RDWR;
            char *filePathName = getFilePath(fileName);
            int logFd = open(filePathName, flags, 0644);

            //2. seek to the offset
            lseek(logFd, 0, SEEK_SET);

            //3. init the file status
            FileStatus *fs = new FileStatus();
            std::shared_ptr<FileStatus> fileStatus(fs);


            //3. read the log and refresh the file status
            int64_t totalReadLength = 0;
            char bufLength[4];
            ssize_t readLength = read(logFd, bufLength, sizeof(bufLength));
            totalReadLength += readLength;
            while (readLength == 4) {
                int32_t dataSize = DecodeFixed32(bufLength);
                LOG(INFO, "FileSystemImpl::catchUpFileStatusFromLog read length = %d, dataSize size =%d", readLength,
                    dataSize);
                std::string logRecord;
                char res[dataSize];

                readLength = read(logFd, res, sizeof(res));
                totalReadLength += readLength;

                logRecord.append(res, dataSize);
                logFormat->deserializeLog(logRecord, fileStatus);
                readLength = read(logFd, bufLength, sizeof(bufLength));
                totalReadLength += readLength;
            }


            // 4. write the new file status to log
            std::string res = logFormat->serializeFileStatusForClose(fileStatus);
            ftruncate(logFd, 0);
            lseek(logFd, 0, SEEK_SET);
            int writeSize = write(logFd, res.data(), res.size());
            close(logFd);

            LOG(INFO, "FileSystemImpl::catchUpFileStatusFromLog out read length = %d", readLength);

            //5. replace the fileStatus in fileStatusMap
            vector<int32_t> pingBlockVector = fileStatusMap[fileName]->getPingIDVector();
            fileStatus->setPingIDVector(pingBlockVector);
            fileStatus->setLastBucket(fileStatusMap[fileName]->getLastBucket());
            fileStatus->setEndOffsetOfBucket(fileStatusMap[fileName]->getEndOffsetOfBucket());
            fileStatusMap[fileName] = fileStatus;
        }

        bool FileSystemImpl::checkFileExist(char *fileName) {
            auto res = fileStatusMap.find(fileName);
            if (res != fileStatusMap.end()) {
                LOG(INFO, "FileSystemImpl::checkFileExist return true");
                return true;
            }
            LOG(INFO, "FileSystemImpl::checkFileExist return false");
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

        }

        void FileSystemImpl::checkAndAddBlockID(char *fileName, std::vector<int32_t> blockIDVector) {
            auto &status = fileStatusMap[fileName];
            std::vector<int32_t> previousVector = status->getBlockIdVector();
            for (int i = 0; i < blockIDVector.size(); i++) {
                int blockIDToCheck = blockIDVector[i];
                vector<int32_t>::iterator itElement = find(previousVector.begin(), previousVector.end(),
                                                           blockIDToCheck);
                if (itElement != previousVector.end()) {
                    int position = distance(previousVector.begin(), itElement);
                    previousVector[position] = -(position + 1);
                }

                previousVector.push_back(blockIDToCheck);
            }
            status->setBlockIdVector(previousVector);


            for (int i = 0; i < previousVector.size(); i++) {
                LOG(INFO, "FileSystemImpl::checkAndAddBlockID, previousVector block = %d,", previousVector[i]);
            }
            LOG(INFO, "********************FileSystemImpl::checkAndAddBlockID check is right***********************");

            /******************************TODO FOR PRINT********************/
            LOG(INFO, "FileSystemImpl::checkAndAddBlockID, block vector size = %d,", status->getBlockIdVector().size());
            for (int i = 0; i < status->getBlockIdVector().size(); i++) {
                LOG(INFO, "FileSystemImpl::checkAndAddBlockID, block = %d,", status->getBlockIdVector()[i]);
            }
            /******************************TODO FOR PRINT********************/

        }


        void FileSystemImpl::updatePingBlockIDOrder(char *fileName) {
            auto &status = fileStatusMap[fileName];
            std::vector<int32_t> previousPingVector = status->getPingIDVector();
            std::vector<int32_t> previousBlockIDVector = status->getBlockIdVector();

            std::vector<int32_t> newPingVector;
            for (int i = 0; i < previousBlockIDVector.size(); i++) {
                for (int j = 0; j < previousPingVector.size(); j++) {
                    if (previousBlockIDVector[i] == previousPingVector[j]) {
                        newPingVector.push_back(previousBlockIDVector[i]);
                        break;
                    }
                }
            }
            status->setPingIDVector(newPingVector);


            /******************************TODO FOR PRINT********************/
            LOG(INFO, "FileSystemImpl::updatePingBlockIDOrder, ping block vector size = %d,",
                status->getPingIDVector().size());
            for (int i = 0; i < status->getPingIDVector().size(); i++) {
                LOG(INFO, "FileSystemImpl::updatePingBlockIDOrder, ping block id = %d,", status->getPingIDVector()[i]);
            }
            /******************************TODO FOR PRINT********************/
        }


        void FileSystemImpl::checkAndAddPingBlockID(char *fileName, std::vector<int32_t> blockIDVector) {

            //1. update the ping vector's order
            updatePingBlockIDOrder(fileName);


            //2. get the inactive blocks
            auto &status = fileStatusMap[fileName];
            std::vector<int32_t> previousPingVector = status->getPingIDVector();
            std::vector<int32_t> newPingVector;
            std::vector<int32_t> inactiveBlockVector;

            LOG(INFO, "FileSystemImpl::checkAndAddPingBlockID previousPingVector size = %d", previousPingVector.size());

            int index = 0;
            for (; index < previousPingVector.size(); index++) {
                if (previousPingVector[index] == status->getLastBucket()) {
                    break;
                } else {
                    LOG(INFO, "FileSystemImpl::checkAndAddPingBlockID inactive block id = %d",
                        previousPingVector[index]);
                    inactiveBlockVector.push_back(previousPingVector[index]);

                }
            }

            //3. set this blocks in shared memory block to inactive.
            inactiveBlock(fileName, inactiveBlockVector);

            //4. set the new ping block vector
            for (; index < previousPingVector.size(); index++) {
                LOG(INFO, "FileSystemImpl::checkAndAddPingBlockID remain active  block id = %d",
                    previousPingVector[index]);
                newPingVector.push_back(previousPingVector[index]);
            }
            newPingVector.insert(newPingVector.end(), blockIDVector.begin(), blockIDVector.end());
            status->setPingIDVector(newPingVector);

            /************************TODO FOR TEST*******************/
            for (int i = 0; i < status->getPingIDVector().size(); i++) {
                LOG(INFO, "FileSystemImpl::checkAndAddPingBlockID in the end the active  block id = %d",
                    status->getPingIDVector()[i]);
            }
            /************************TODO FOR TEST*******************/



        }


        void FileSystemImpl::acquireNewBlock(char *fileName) {

            //1. set the shared memory
            vector<int> blockIDVector = sharedMemoryManager->acquireNewBlock(fileName);
            if (blockIDVector.size() == 0) {
                LOG(LOG_ERROR, "FileSystemImpl::acquireNewBlock, do not have blocks which type='0' ");
            }
            assert(blockIDVector.size() == QUOTA_SIZE);

            //2.  write the new file status to Log.
            string res = logFormat->serializeLog(blockIDVector, LogFormat::RecordType::acquireNewBlock);
//            LOG(INFO, "8, LogFormat res size = %d", res.size());
            writeFileStatusToLog(fileName, res);



            //TODO FOR TEST***************
            LOG(INFO, "FileSystemImpl::acquireNewBlock before 1 file status");
            sharedMemoryManager->printSMStatus();
            LOG(INFO, "FileSystemImpl::acquireNewBlock before 2 file status");
            //TODO FOR TEST***************

            //3. check the block vector size is enough or not, if not,see the shared memory's type =2, and acquire more blocks
            if (blockIDVector.size() < QUOTA_SIZE) {
                int remainNeedBlocks = QUOTA_SIZE - blockIDVector.size();
                std::vector<int32_t> remainBlockVector = sharedMemoryManager->getBlocksWhichTypeEqual2(
                        remainNeedBlocks);
                int totalSize = remainBlockVector.size() + blockIDVector.size();

                LOG(INFO, "FileSystemImpl::acquireNewBlock, acquire %d blocks which type = '2' ",
                    remainBlockVector.size());

                if (totalSize == 0) {
                    LOG(LOG_ERROR, "FileSystemImpl::acquireNewBlock, acquire blocks fail, do not acquire any blocks");
                }

                if (totalSize < QUOTA_SIZE) {
                    LOG(LOG_ERROR,
                        "FileSystemImpl::acquireNewBlock, do not have enough blocks, only get %d number of blocks",
                        totalSize);
                    if (totalSize == 0) {
                        return;
                    }
                }

                //TODO FOR TEST***************
                LOG(INFO, "FileSystemImpl::acquireNewBlock middle 1 file status");
                sharedMemoryManager->printSMStatus();
                LOG(INFO, "FileSystemImpl::acquireNewBlock middle 2 file status");
                //TODO FOR TEST***************

                evictBlock(fileName, remainBlockVector);


                //TODO FOR TEST***************
                LOG(INFO, "FileSystemImpl::acquireNewBlock after 1 file status");
                sharedMemoryManager->printSMStatus();
                LOG(INFO, "FileSystemImpl::acquireNewBlock after 2 file status");
                //TODO FOR TEST***************


                blockIDVector.insert(blockIDVector.end(), remainBlockVector.begin(), remainBlockVector.end());
            }


            //4. set the first block to the lastBucket
            auto &status = fileStatusMap[fileName];
            if (status->getBlockIdVector().size() == 0 && blockIDVector.size() > 0) {
                LOG(INFO,
                    "this is the first time acquire the new block, so set the last bucket to the first block id that acquired");
                status->setLastBucket(blockIDVector[0]);
            }

            //5. check and add the new acquired block id
            checkAndAddBlockID(fileName, blockIDVector);

            //6. set the ping block id
            checkAndAddPingBlockID(fileName, blockIDVector);

            for (int i = 0; i < blockIDVector.size(); i++) {
                LOG(INFO, "the acquired id block list index = %d, blockID = %d ", i, blockIDVector[i]);
            }

        }


        void FileSystemImpl::inactiveBlock(char *fileName, const std::vector<int32_t> &blockIdVector) {
            //1. set the shared memory
            for (int i = 0; i < blockIdVector.size(); i++) {
                int blockID = blockIdVector[i];
                int index = getIndexAccordingBlockID(fileName, blockID);
                sharedMemoryManager->inactiveBlock(blockIdVector[i], index);
            }

            //2. change the ping block list
            auto &status = fileStatusMap[fileName];
            vector<int32_t> vecPrevious = status->getPingIDVector();
            vector<int32_t> val = deleteVector(vecPrevious, blockIdVector);
            status->setPingIDVector(val);

            //3.  write the new file status to Log.
            string res = logFormat->serializeLog(blockIdVector, LogFormat::RecordType::inactiveBlock);
            writeFileStatusToLog(fileName, res);

        }

        std::vector<int32_t>
        FileSystemImpl::deleteVector(std::vector<int32_t> previousVector, std::vector<int32_t> toDeleteVector) {
            for (int i = 0; i < toDeleteVector.size(); i++) {
                int blockIDToDelete = toDeleteVector[i];
                std::vector<int32_t>::iterator it;
                for (it = previousVector.begin(); it != previousVector.end();) {
                    if (*it == blockIDToDelete) {
                        it = previousVector.erase(it);
                        break;
                    } else {
                        it++;
                    }
                }
            }
            return previousVector;
        }

        void FileSystemImpl::releaseBlock(char *fileName, const std::vector<int32_t> &blockIdVector) {
            //1. set the shared memory
            for (int i = 0; i < blockIdVector.size(); i++) {
                sharedMemoryManager->releaseBlock(blockIdVector[i]);
            }

            auto &status = fileStatusMap[fileName];

            //3. release the block in the blockIDVector
            vector<int32_t> vecBefore = status->getBlockIdVector();
            vector<int32_t> resVector = deleteVector(vecBefore, blockIdVector);
            status->setBlockIdVector(resVector);


            //4. release the block in the pingIDVector
            vector<int32_t> vecBeforePing = status->getPingIDVector();
            vector<int32_t> resPing = deleteVector(vecBeforePing, blockIdVector);
            status->setPingIDVector(resPing);

            //5.  write the new file status to Log.
            string res = logFormat->serializeLog(blockIdVector, LogFormat::RecordType::releaseBlock);
            writeFileStatusToLog(fileName, res);

        }

        void FileSystemImpl::evictBlock(char *fileName, const std::vector<int32_t> &blockIdVector) {
            for (int i = 0; i < blockIdVector.size(); i++) {
                //1. get the previous fileName of the block id
                int tmpBlockID = blockIdVector[i];
                string previousFileName = sharedMemoryManager->getFileNameAccordingBlockID(tmpBlockID);
                LOG(INFO, "FileSystemImpl::evictBlock, previousFileName=%s", previousFileName.data());

                //2. get the previous block index
                int index = sharedMemoryManager->getBlockIDIndex(tmpBlockID);
                index = index + 1;

                //3&4. 3.set the shared memory 2->1;   4. change the file name
                sharedMemoryManager->evictBlock(tmpBlockID, fileName);


                //6. write data to oss;
                writeDate2OSS((char *) previousFileName.data(), tmpBlockID, index);


                //5. write the previous file log
                LOG(INFO, "FileSystemImpl::evictBlock, before index = %d", index);
                std::vector<int32_t> previousVector;
                previousVector.push_back(-index);
                string previousRes = logFormat->serializeLog(previousVector, LogFormat::RecordType::remoteBlock);
                writeFileStatusToLog((char *) previousFileName.data(), previousRes);


            }

            //8.  write the new file status to Log.
            string res = logFormat->serializeLog(blockIdVector, LogFormat::RecordType::evictBlock);
            writeFileStatusToLog(fileName, res);

        }


        std::string FileSystemImpl::constructFileKey(std::string str, int index) {
            stringstream ss;
            ss << str << "-" << index;
            return ss.str();
        }


        void FileSystemImpl::writeDate2OSS(char *fileName, int blockID, int index) {
            //1. parpre qingstor context
            string fileKey;
            fileKey.append(fileName, strlen(fileName));
            fileKey = constructFileKey(fileKey, index);
            qsReadWrite->getPutObject((char *) fileKey.data());

            LOG(INFO, "FileSystemImpl::writeDate2OSS, index = %d, fileKey = %s, blockID=%d", index, fileKey.data(),
                blockID);


            char *buffer = (char *) malloc(READ_BUFFER_SIZE * sizeof(char));
            const int iter = (int) (SIZE_OF_BLOCK / READ_BUFFER_SIZE);

            int64_t baseOffsetInBucket = blockID * SIZE_OF_BLOCK;
            fsSeek(baseOffsetInBucket, SEEK_SET);

            int j;
            for (j = 0; j < iter; j++) {
                int64_t readBytes = readDataFromBucket(buffer, READ_BUFFER_SIZE);
                LOG(INFO, "FileSystemImpl::writeDate2OSS, readBytes = %d", readBytes);
                if (readBytes <= 0) {
                    break;
                }
                qsReadWrite->qsWrite((char *) fileKey.data(), buffer, readBytes);
                baseOffsetInBucket += READ_BUFFER_SIZE;
                fsSeek(baseOffsetInBucket, SEEK_SET);
            }

            //2. delete qingstor context
            qsReadWrite->closePutObject();
        }


        int FileSystemImpl::getIndexAccordingBlockID(char *fileName, int blockID) {
//            LOG(INFO, "FileSystemImpl::getIndexAccordingBlockID fileName=%s", fileName);
            auto &fileStatus = fileStatusMap[fileName];
            if (!fileStatus) {
                LOG(LOG_ERROR,
                    "FileSystemImpl::getIndexAccordingBlockID. fileStatusMap do not contain the file which name  is = %s",
                    fileName);
                return NULL;
            }

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

            /*************TODO FOR TEST**************/
            int64_t res = write(bucketFd, buf, size);
            LOG(INFO, "FileSystemImpl::writeDataToBucket. res=%d", res);

            if (res == -1) {
                LOG(LOG_ERROR, "some error occur, can not write to the ssd file");
            }
        }


        void FileSystemImpl::checkBucketFileOpen() {
            int flags = O_CREAT | O_RDWR;
            if (bucketFd == -1) {
                LOG(LOG_ERROR, "the ssd file do not open, so open it");
                bucketFd = open(BUCKET_PATH_FILE_NAME, flags, 0644);
            }
        }

        int32_t FileSystemImpl::fsSeek(int64_t offset, int whence) {
            checkBucketFileOpen();
            int64_t res = lseek(bucketFd, offset, whence);
            if (res == -1) {
                LOG(LOG_ERROR, "fsSeek error");
            }
        }


        void FileSystemImpl::closeBucketFile() {
            close(bucketFd);
        }

        void FileSystemImpl::stopSystem() {
            closeBucketFile();
            sharedMemoryManager->closeSMFile();
            sharedMemoryManager->closeSMBucket();

            qsReadWrite->destroyContext();
        }

        //TODO, flush, write log and so on
        void FileSystemImpl::closeFile(char *fileName) {
            auto &status = fileStatusMap[fileName];

            //2. release the empty block
            std::vector<int32_t> blockVector = status->getBlockIdVector();
            int lastBucket = status->getLastBucket();
            LOG(INFO, "FileSystemImpl::closeFile. lastBucket = %d,blockVector.size= %d", lastBucket,
                blockVector.size());
            int i = 0;
            for (; i < blockVector.size(); i++) {
                if (lastBucket == blockVector[i]) {
                    LOG(INFO, "FileSystemImpl::closeFile. i=%d", i);
                    i++;
                    break;
                }
            }

            std::vector<int32_t> emptyBlockVector;
            if (i < blockVector.size()) {
                LOG(INFO, "FileSystemImpl::closeFile come in IF STATEMENT the file");
                for (; i < blockVector.size(); i++) {
                    LOG(INFO, "FileSystemImpl::closeFile come in second IF STATEMENT");
                    emptyBlockVector.push_back(blockVector[i]);
                }
                releaseBlock(fileName, emptyBlockVector);
            }


            //3. BUG FIX. if the ping block have been release, this block should not be inactive
            std::vector<int32_t> pingBlockIDVector = status->getPingIDVector();
            std::vector<int32_t> newPingBlockVector = deleteVector(pingBlockIDVector, emptyBlockVector);


            //4. set the shared memory, inactive the block
            for (int i = 0; i < newPingBlockVector.size(); i++) {
                if (newPingBlockVector[i] >= 0) {
                    int blockID = newPingBlockVector[i];
                    int index = getIndexAccordingBlockID(fileName, blockID);
                    sharedMemoryManager->inactiveBlock(blockID, index);
                }
            }

            //4. write the close status to log

            //4.1. open file
            int flags = O_CREAT | O_RDWR;
            char *filePathName = getFilePath(fileName);
            int logFd = open(filePathName, flags, 0644);

            //4.2. rebuild the file status from log
            FileStatus *fs = new FileStatus();
            std::shared_ptr<FileStatus> rebuildFileStatus(fs);

            //4.3. read the log and refresh the file status
            char bufLength[4];
            int32_t length = read(logFd, bufLength, sizeof(bufLength));
            while (length == 4) {
                int32_t dataSize = DecodeFixed32(bufLength);
                LOG(INFO, "FileSystemImpl::closeFile read length = %d, dataSize size =%d", length,
                    dataSize);
                std::string logRecord;
                char res[dataSize];
                read(logFd, res, sizeof(res));
                logRecord.append(res, dataSize);
                logFormat->deserializeLog(logRecord, rebuildFileStatus);
                length = read(logFd, bufLength, sizeof(bufLength));
            }
            rebuildFileStatus->setEndOffsetOfBucket(status->getEndOffsetOfBucket());

            LOG(INFO, "FileSystemImpl::closeFile out read length = %d", length);

            //5. write the close status to log
            std::string res = logFormat->serializeFileStatusForClose(rebuildFileStatus);
            ftruncate(logFd, 0);
            lseek(logFd, 0, SEEK_SET);
            int writeSize = write(logFd, res.data(), res.size());
            close(logFd);
        }


        //TODO JUST FOR TEST
        void FileSystemImpl::readCloseFileStatus(char *fileName) {
//            LOG(INFO, "***********************readCloseFileStatus*********************************");

            FileStatus *fs = new FileStatus();
            std::shared_ptr<FileStatus> fileStatus(fs);

            int flags = O_CREAT | O_RDWR;

            char *filePathName = getFilePath(fileName);
            int logFd = open(filePathName, flags, 0644);

            char bufLength[4];
            int32_t length = read(logFd, bufLength, sizeof(bufLength));
            while (length == 4) {
                int32_t dataSize = DecodeFixed32(bufLength);
//                LOG(INFO, "FileSystemImpl::readCloseFileStatus read length = %d, dataSize size =%d", length, dataSize);
                std::string logRecord;
                char res[dataSize];
                read(logFd, res, sizeof(res));
                logRecord.append(res, dataSize);
                logFormat->deserializeLog(logRecord, fileStatus);
                length = read(logFd, bufLength, sizeof(bufLength));
            }
            fileStatus->setFileName(fileName);

            LOG(INFO,
                "*********************** in the end, before the close File Status*********************************");
            int64_t endOffsetOfBucket = fileStatus->getEndOffsetOfBucket();
            vector<int32_t> blockIDVector = fileStatus->getBlockIdVector();
            LOG(INFO, "endOffsetOfBucket=%d,blockIDVector.size=  %d", endOffsetOfBucket, blockIDVector.size());
            for (int i = 0; i < blockIDVector.size(); i++) {
                LOG(INFO, " block id = %d", blockIDVector[i]);
            }

            LOG(INFO,
                "*********************** in the end, after the close File Status*********************************");
            close(logFd);


            //read data from the block id lists without change the cache
            readTotalDataFromFile(fileStatus);

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
//            LOG(INFO, "filePathName = %s", filePathName);

            int logFd = open(filePathName, flags, 0644);
//            LOG(INFO, "9, LogFormat dataStr size = %d", dataStr.size());


            int32_t length = write(logFd, dataStr.data(), dataStr.size());

            close(logFd);

//            LOG(INFO, "FileSystemImpl::writeFileStatusToLog write size = %d", length);

        }


        //TODO ,JUST FOR TEST
        int64_t FileSystemImpl::readTotalDataFromFile(std::shared_ptr<FileStatus> fileStatus) {
            int SIZE = 128;
            char *readBuf = new char[SIZE];
            int iter = SIZE_OF_BLOCK / SIZE;
            std::string fileName = fileStatus->getFileName();

            stringstream ss;
            ss << "/ssdfile/ssdkv/" << fileName << "-readTotalDataFromFile";

            string filePath = ss.str();

            vector<int32_t> blockIDVector = fileStatus->getBlockIdVector();
            LOG(INFO, "1. FileSystemImpl::readTotalDataFromFile. filePath=%s. block vector size = %d", filePath.c_str(),
                blockIDVector.size());

            for (int i = 0; i < blockIDVector.size(); i++) {
                int blockID = blockIDVector[i];
                LOG(INFO, "2. FileSystemImpl::readTotalDataFromFile. blockID=%d, i=%d", blockID, i);
                if (i == blockIDVector.size() - 1) {
                    LOG(INFO, "3. FileSystemImpl::readTotalDataFromFile. come in if");

                    int64_t endOfBucketOffset = fileStatus->getEndOffsetOfBucket();

                    if (blockID >= 0) {
                        fsSeek(blockID * SIZE_OF_BLOCK, SEEK_SET);
                        while (endOfBucketOffset > 0) {
                            int64_t leftData = SIZE <= endOfBucketOffset ? SIZE : endOfBucketOffset;
                            LOG(INFO,
                                "4. FileSystemImpl::readTotalDataFromFile. come in if. leftData=%d,endOfBucketOffset=%d",
                                leftData, endOfBucketOffset);

                            int64_t readData = readDataFromBucket(readBuf, leftData);
                            writeUtil(filePath, readBuf, readData);
                            readBuf = new char[SIZE];
                            endOfBucketOffset -= readData;
                        }

                    } else {
                        std::string ossFileName = constructFileKey(fileName, -blockID);
                        LOG(INFO,
                            "5. FileSystemImpl::readTotalDataFromFile. come in if. ossFileName = %s",
                            ossFileName.c_str());

                        qsReadWrite->getGetObject((char *) ossFileName.data());
                        while (endOfBucketOffset > 0) {
                            int64_t leftData = SIZE <= endOfBucketOffset ? SIZE : endOfBucketOffset;
                            LOG(INFO,
                                "5. FileSystemImpl::readTotalDataFromFile. come in if. leftData=%d,endOfBucketOffset=%d",
                                leftData, endOfBucketOffset);

                            int64_t readData = qsReadWrite->qsRead((char *) ossFileName.c_str(), readBuf, leftData);
                            writeUtil(filePath, readBuf, readData);
                            readBuf = new char[SIZE];
                            endOfBucketOffset -= readData;
                        }

                        qsReadWrite->closeGetObject();
                    }

                } else {
                    LOG(INFO, "FileSystemImpl::readTotalDataFromFile. come in else");
                    if (blockID >= 0) {
                        fsSeek(blockID * SIZE_OF_BLOCK, SEEK_SET);
                        for (int j = 0; j < iter; j++) {
                            readDataFromBucket(readBuf, SIZE);
                            LOG(INFO, "7. FileSystemImpl::readTotalDataFromFile. come in if. j=%d, readBuf=%s", j,
                                readBuf);
                            writeUtil(filePath, readBuf, SIZE);
                            readBuf = new char[SIZE];
                        }
                    } else {
                        std::string ossFileName = constructFileKey(fileName, -blockID);
                        qsReadWrite->getGetObject((char *) ossFileName.data());
                        for (int j = 0; j < iter; j++) {
                            LOG(INFO, "8. FileSystemImpl::readTotalDataFromFile. come in if. j=%d, ossFileName=%s", j,
                                ossFileName.c_str());
                            qsReadWrite->qsRead((char *) ossFileName.c_str(), readBuf, SIZE);
                            LOG(INFO, "8. FileSystemImpl::readTotalDataFromFile. readBuf=%s", readBuf);
                            writeUtil(filePath, readBuf, SIZE);
                            readBuf = new char[SIZE];
                        }
                        qsReadWrite->closeGetObject();
                    }
                }
            }
        }

        //TODO ,JUST FOR TEST
        void FileSystemImpl::writeUtil(string fileName, char *buf, int64_t size) {
            std::ofstream ostrm(fileName, std::ios::out | std::ios::app);
            LOG(INFO, "FileSystemImpl::writeUtil .fileName=%s,  buf=%s", fileName.c_str(), buf);
            ostrm.write(buf, size);
        }


    }

}