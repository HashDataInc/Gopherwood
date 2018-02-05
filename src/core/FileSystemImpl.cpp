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


        void FileSystemImpl::deleteBlockFromOSS(int64_t ossindex, string fileName) {
            string ossFileName = constructFileKey(fileName, ossindex + 1);
            qsReadWrite->qsDeleteObject((char *) ossFileName.c_str());
        }


        //TODO ******************************
        void FileSystemImpl::writeDataFromOSS2Bucket(int64_t ossindex, string fileName) {
            //1. prapre qingstor context
            string ossFileName = constructFileKey(fileName, ossindex + 1);
            qsReadWrite->getGetObject((char *) ossFileName.data());

            // 1. get the block which can write, and seek to the begin of the block.
            int blockID = getOneBlockForWrite(ossindex, fileName);
            auto &status = fileStatusMap[fileName];

            int64_t totalOffset = blockID * SIZE_OF_BLOCK;


            LOG(INFO, "FileSystemImpl::writeDataFromOSS2Bucket. blockID=%d,", blockID);
            //2. read data from OSS, and write the data to bucket.
            char buf[SIZE_OF_BLOCK / 8];
            int64_t readLength = qsReadWrite->qsRead((char *) ossFileName.data(), buf, sizeof(buf));
            while (readLength > 0) {
                LOG(INFO, "FileSystemImpl::writeDataFromOSS2Bucket. readLength=%d, buf = %s", readLength, buf);

                //1. get the lock
                sharedMemoryManager->getLock();
                fsSeek(totalOffset, SEEK_SET);
                writeDataToBucket(buf, readLength);
                sharedMemoryManager->releaseLock();
                //2. release the lock

                totalOffset += readLength;


                readLength = qsReadWrite->qsRead((char *) ossFileName.data(), buf, sizeof(buf));

            }

            qsReadWrite->closeGetObject();

            //3. delete the data in oss.
            qsReadWrite->qsDeleteObject((char *) ossFileName.c_str());

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
            int nextBlockIndex = getIndexAccordingBlockID((char *) fileName.data(), blockID) + 1;
            LOG(INFO,
                "FileSystemImpl::getOneBlockForWrite, before nextBlockIndex  = %d,status->getBlockIdVector().size()=%d",
                nextBlockIndex, status->getBlockIdVector().size());


            //2. check the newBlockID is pin or not
            vector<int32_t> blockIDVector = status->getBlockIdVector();
            int newBlockID = blockIDVector[nextBlockIndex];
            bool isExist = status->getLruCache()->exists(newBlockID);;


            while (!(nextBlockIndex < status->getBlockIdVector().size() && isExist)) {
                LOG(LOG_ERROR,
                    "FileSystemImpl::getOneBlockForWrite, ACQUIRE ZERO BLOCKS, SO ACQUIRE AGAIN UNTIL GET ONE");
                acquireNewBlock((char *) fileName.data());

                // 3. regain the last block id
                blockID = status->getLastBucket();
                nextBlockIndex = getIndexAccordingBlockID((char *) fileName.data(), blockID) + 1;

                LOG(INFO,
                    "FileSystemImpl::getOneBlockForWrite, nextBlockIndex  = %d,status->getBlockIdVector().size()=%d",
                    nextBlockIndex, status->getBlockIdVector().size());

                // 4. recheck the newBlockID is pin or not
                blockIDVector = status->getBlockIdVector();

                LOG(INFO, "FileSystemImpl::getOneBlockForWrite, blockIDVector size  = %d", blockIDVector.size());

                if (nextBlockIndex < blockIDVector.size()) {
                    newBlockID = blockIDVector[nextBlockIndex];
                    isExist = status->getLruCache()->exists(newBlockID);
                    LOG(INFO, "FileSystemImpl::getOneBlockForWrite, isExist  = %d,newBlockID=%d", isExist, newBlockID);
                }
            }
            LOG(INFO,
                "FileSystemImpl::getOneBlockForWrite, after nextBlockIndex  = %d, status->getBlockIdVector().size()=%d",
                nextBlockIndex, status->getBlockIdVector().size());



            /******************TODO FOR TEST****************************/
            for (int i = 0; i < blockIDVector.size(); i++) {
                LOG(INFO, "FileSystemImpl::getOneBlockForWrite, before blockIDVector[i] = %d,", blockIDVector[i]);
            }
            /******************TODO FOR TEST****************************/



            // 2. check whether the negative blockID is the last block ID
            // BUG-FIX. there just need to update the lastBucket, but not needed updated the lastBucketIndex.
            // becase the last bucket index do not change.
            // specifically. the code here </code> status->setLastBucketIndex(nextBlockIndex)</code> is WRONG.
            // because, the last bucket index is not change, the nextBlockIndex is the the last bucket id's next index.
            int tmpBlockID = -(ossindex + 1);
            if (status->getLastBucket() == tmpBlockID) {
                status->setLastBucket(newBlockID);
                LOG(INFO,
                    "FileSystemImpl::getOneBlockForWrite. the negative blockID is the last block ID, so replace it");
            }


            //3. use the newBlockID to replace the negative block id in OSS
            blockIDVector[ossindex] = newBlockID;


            //4. change the lru cache
            status->getLruCache()->put(newBlockID, newBlockID);

            //5.delete it from block id vector
            blockIDVector.erase(blockIDVector.begin() + nextBlockIndex);
            status->setBlockIdVector(blockIDVector);


            /******************TODO FOR TEST****************************/
            for (int i = 0; i < blockIDVector.size(); i++) {
                LOG(INFO, "FileSystemImpl::getOneBlockForWrite, after blockIDVector[i] = %d,", blockIDVector[i]);
            }

            status->getLruCache()->printLruCache();
            /******************TODO FOR TEST****************************/


            return newBlockID;
        }


        /**
         * check whether the blockID belongs to the file or not,
         */
        bool FileSystemImpl::checkBlockIDWithFileName(int blockID, string fileName) {
            bool isNotKicked = sharedMemoryManager->checkFileNameAndTypeAndSetKick(blockID, fileName);
            return isNotKicked;
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


//            std::vector<int32_t> pingBlockVector;
            int quotaCount = 0;

            int blockIDSize = fileStatus->getBlockIdVector().size();

            if (blockIDSize > 0) {
                //2. set last bucket
                int lastBlockID = fileStatus->getBlockIdVector()[blockIDSize - 1];

                //4. set the last bucket
                fileStatus->setLastBucket(lastBlockID);
                fileStatus->setLastBucketIndex(blockIDSize - 1);

                //5. add the last bucket to ping block if its not in the OSS
                if (lastBlockID >= 0 && sharedMemoryManager->checkFileNameAndTypeAndSetKick(lastBlockID, fileName)) {

                    LOG(INFO, "FileSystemImpl::rebuildFileStatusFromLog. lastBlockID >= 0 and return true ");
                    //5.1 change the shared memory .NO NEED, BECAUSE checkFileNameAndTypeAndSetKick(), have been done

                    //5.2 add to the lru cache
//                    pingBlockVector.push_back(lastBlockID);
                    fileStatus->getLruCache()->put(lastBlockID, lastBlockID);
                    quotaCount++;
                }

            }

            //3. set ping block vector
            int blockIndex = 0;
            while ((blockIndex < blockIDSize) && (quotaCount < MIN_QUOTA_SIZE)) {
                int tmpBlockID = fileStatus->getBlockIdVector()[blockIndex];
                int lastBlockID = fileStatus->getLastBucket();
                if ((tmpBlockID >= 0) && (tmpBlockID != lastBlockID) &&
                    (sharedMemoryManager->checkFileNameAndTypeAndSetKick(tmpBlockID, fileName))) {
                    //3.1 change the shared memory, NO NEED, BECAUSE checkFileNameAndTypeAndSetKick(), have been done

                    LOG(INFO, "FileSystemImpl::rebuildFileStatusFromLog. tmpBlockID >= 0 and return true ");

                    //3.2 add to the  lru cache
                    fileStatus->getLruCache()->put(tmpBlockID, tmpBlockID);
//                    pingBlockVector.push_back(tmpBlockID);
                    quotaCount++;
                }
                blockIndex++;
            }
//            fileStatus->setPingIDVector(pingBlockVector);


            //4. set the fileName
            fileStatus->setFileName(fileName);

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
        void FileSystemImpl::catchUpFileStatusFromLog(char *fileName) {

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

            //5. set the lru cache
            fileStatus->setLruCache(fileStatusMap[fileName]->getLruCache());

            //6. set the fileType
            fileStatus->setAccessFileType(fileStatusMap[fileName]->getAccessFileType());

            //7. set the last bucket
            int lastBucketIndex = fileStatusMap[fileName]->getLastBucketIndex();
            std::vector<int32_t> blockIDVector = fileStatus->getBlockIdVector();
            if (lastBucketIndex < blockIDVector.size()) {
                LOG(INFO, "FileSystemImpl::catchUpFileStatusFromLog. the lastBlockIndex=%d, and the lastBlockID=%d",
                    lastBucketIndex, blockIDVector[lastBucketIndex]);
                fileStatus->setLastBucket(blockIDVector[lastBucketIndex]);
            } else {
                LOG(LOG_ERROR,
                    "FileSystemImpl::catchUpFileStatusFromLog. the last bucket index=%d is larger than the blockIDVector size=%d",
                    lastBucketIndex, blockIDVector.size());
            }

            //8. set the end of the offset
            fileStatus->setEndOffsetOfBucket(fileStatusMap[fileName]->getEndOffsetOfBucket());

            //9. replace the fileStatus in fileStatusMap
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

                    //BUG-FIX. this will change the last bucket. eg. if the blockIDToCheck is the last bucket before,
                    // so when check the block, we should also check the block is the last block or not.
                    if (blockIDToCheck == status->getLastBucket()) {
                        status->setLastBucket(-(position + 1));
                        status->setLastBucketIndex(position);
                    }
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


        // DELETE. NOT NEED ANYMORE?   this code is tuning the ping block vector's sequence is the same as the block id's sequence.
        //for example, the block list id is 0,-2,-3,-4,1,2. and at the same time the ping block id vector is 2,0 and we tune it to 0,2
        // but in the lru model ,this code can be deleted.

        /**
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


            //******************************TODO FOR PRINT********************
            LOG(INFO, "FileSystemImpl::updatePingBlockIDOrder, ping block vector size = %d,",
                status->getPingIDVector().size());
            for (int i = 0; i < status->getPingIDVector().size(); i++) {
                LOG(INFO, "FileSystemImpl::updatePingBlockIDOrder, ping block id = %d,", status->getPingIDVector()[i]);
            }
            //******************************TODO FOR PRINT********************
        }
         **/


        void FileSystemImpl::checkAndAddPingBlockID(char *fileName, std::vector<int32_t> blockIDVector) {

            //1. add the ping block id to the lru cache
            auto &status = fileStatusMap[fileName];
            for (int i = 0; i < blockIDVector.size(); i++) {
                int tmpBlockID = blockIDVector[i];
                //1.1 update the lru cache
                std::vector<int32_t> inactiveBlockVector = status->getLruCache()->put(tmpBlockID, tmpBlockID);


                //2. set this blocks in shared memory block to inactive.
                if (inactiveBlockVector.size() > 0) {

                    // BUG-FIX. in the lru cache. the block ID maybe empty or full. so should be checked first.

                    std::vector<int32_t> emptyBlockVector;
                    std::vector<int32_t> fullBlockVector;
                    int lastBlockIndex = getIndexAccordingBlockID(fileName, status->getLastBucket());
                    for (int j = 0; j < inactiveBlockVector.size(); j++) {
                        int tmpBlockID = inactiveBlockVector[j];
                        int index = getIndexAccordingBlockID(fileName, tmpBlockID);
                        if (index > lastBlockIndex) {
                            emptyBlockVector.push_back(tmpBlockID);
                            LOG(INFO,
                                "FileSystemImpl::checkAndAddPingBlockID. the evict block is empty, and the blockID =%d, lastBlock =%d.",
                                tmpBlockID, status->getLastBucket());
                        } else {
                            fullBlockVector.push_back(tmpBlockID);
                            LOG(INFO,
                                "FileSystemImpl::checkAndAddPingBlockID. the evict block is full, and the blockID =%d, lastBlock =%d.",
                                tmpBlockID, status->getLastBucket());
                        }
                    }

                    // inactive the full block ID
                    inactiveBlock(fileName, fullBlockVector);

                    //release the empty block
                    releaseBlock(fileName, emptyBlockVector);
                }
            }
        }


        void FileSystemImpl::acquireNewBlock(char *fileName) {

            //1. set the shared memory
            vector<int> blockIDVector = sharedMemoryManager->acquireNewBlock(fileName);
            if (blockIDVector.size() == 0) {
                LOG(LOG_ERROR, "FileSystemImpl::acquireNewBlock, do not have blocks which type='0' ");
            }
            assert(blockIDVector.size() == MIN_QUOTA_SIZE);

            //2.  write the new file status to Log.
            if (blockIDVector.size() > 0) {
                string res = logFormat->serializeLog(blockIDVector, LogFormat::RecordType::acquireNewBlock);
                writeFileStatusToLog(fileName, res);
            }



            //TODO FOR TEST***************
            LOG(INFO, "FileSystemImpl::acquireNewBlock before 1 file status");
            sharedMemoryManager->printSMStatus();
            LOG(INFO, "FileSystemImpl::acquireNewBlock before 2 file status");
            //TODO FOR TEST***************

            //3. check the block vector size is enough or not, if not,see the shared memory's type =2, and acquire more blocks
            if (blockIDVector.size() < MIN_QUOTA_SIZE) {
                int remainNeedBlocks = MIN_QUOTA_SIZE - blockIDVector.size();

                std::unordered_map<int, std::string> remainBlockStatusMap = sharedMemoryManager->getBlocksWhichTypeEqual2(
                        remainNeedBlocks);
                int totalSize = remainBlockStatusMap.size() + blockIDVector.size();

                LOG(INFO, "FileSystemImpl::acquireNewBlock, acquire %d blocks which type = '2' ",
                    remainBlockStatusMap.size());

                if (totalSize == 0) {
                    LOG(LOG_ERROR, "FileSystemImpl::acquireNewBlock, acquire blocks fail, do not acquire any blocks");
                }

                if (totalSize < MIN_QUOTA_SIZE) {
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

                std::vector<int> noEvictBlockVector = evictBlock(fileName, remainBlockStatusMap);

                //TODO FOR TEST***************
                LOG(INFO, "FileSystemImpl::acquireNewBlock after 1 file status");
                sharedMemoryManager->printSMStatus();
                LOG(INFO, "FileSystemImpl::acquireNewBlock after 2 file status");
                //TODO FOR TEST***************


                std::vector<int> remainBlockVector;
                for (const auto &blockStatus: remainBlockStatusMap) {
                    remainBlockVector.push_back(blockStatus.first);
                }
                std::vector<int> newRemainBlockVector = deleteVector(remainBlockVector, noEvictBlockVector);

                blockIDVector.insert(blockIDVector.end(), newRemainBlockVector.begin(), newRemainBlockVector.end());
            }


            //4. set the first block to the lastBucket
            auto &status = fileStatusMap[fileName];
            if (status->getBlockIdVector().size() == 0 && blockIDVector.size() > 0) {
                LOG(INFO,
                    "this is the first time acquire the new block, so set the last bucket to the first block id that acquired");
                status->setLastBucket(blockIDVector[0]);
                status->setLastBucketIndex(0);
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

            //3.  write the new file status to Log.
            if (blockIdVector.size() > 0) {
                string res = logFormat->serializeLog(blockIdVector, LogFormat::RecordType::inactiveBlock);
                writeFileStatusToLog(fileName, res);
            }
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


            //4. release the block in the lru cache
            for (int i = 0; i < blockIdVector.size(); i++) {
                int tmpBlockID = blockIdVector[i];
                status->getLruCache()->deleteObject(tmpBlockID);
            }

            //5.  write the new file status to Log.
            if (blockIdVector.size() > 0) {
                string res = logFormat->serializeLog(blockIdVector, LogFormat::RecordType::releaseBlock);
                writeFileStatusToLog(fileName, res);
            }

        }

        void FileSystemImpl::deleteBlockFromSSD(char *fileName, const std::vector<int32_t> &blockIdVector) {
            //1. set the shared memory
            for (int i = 0; i < blockIdVector.size(); i++) {
                sharedMemoryManager->releaseBlock(blockIdVector[i]);
            }

            auto &status = fileStatusMap[fileName];


            //2. release the block in the lru cache
            for (int i = 0; i < blockIdVector.size(); i++) {
                int tmpBlockID = blockIdVector[i];
                status->getLruCache()->deleteObject(tmpBlockID);
            }
        }


        std::vector<int>
        FileSystemImpl::evictBlock(char *fileName, std::unordered_map<int, std::string> blockStatusMap) {

            std::vector<int> noEvictBlockVector;

            for (const auto &blockStatus: blockStatusMap) {
                //1. get the previous fileName of the block id
                int tmpBlockID = blockStatus.first;
                std::string previousFileName = sharedMemoryManager->getFileNameAccordingBlockID(tmpBlockID);
                LOG(INFO, "FileSystemImpl::evictBlock, previousFileName=%s", previousFileName.data());

                //2. get the previous block index
                int blockIndex = sharedMemoryManager->getBlockIDIndex(tmpBlockID);
                blockIndex = blockIndex + 1;


                //3. construct the fileNameInOSS in OSS
                stringstream ss;
                string fileNameInOSS;
                fileNameInOSS.append(previousFileName.c_str(), strlen(previousFileName.c_str()));
                int processPID = getpid();

                ss << fileNameInOSS << "-" << blockIndex;
                fileNameInOSS = ss.str();


                ss << "-" << processPID;
                string fileNameInOSStmp = ss.str();;

                LOG(INFO, "FileSystemImpl::evictBlock, fileNameInOSS=%s, fileNameInOSStmp=%s.", fileNameInOSS.c_str(),
                    fileNameInOSStmp.c_str());

                LOG(INFO,
                    "FileSystemImpl::evictBlock, previousFileName=%s, blockIndex=%d, processPID=%d, fileNameInOSS=%s",
                    previousFileName.c_str(), blockIndex, processPID, fileNameInOSS.c_str());

                //3. write data to oss;
                writeDate2OSS((char *) fileNameInOSStmp.c_str(), tmpBlockID);





                //4. BEGIN A TRANSACTION
                //4.1 get lock
                sharedMemoryManager->getLock();

                //4.2 check the block status is right or not
                std::string nowBlockStatus = sharedMemoryManager->getBlockStatus(tmpBlockID);

                LOG(INFO, "FileSystemImpl::evictBlock. nowBlockStatus = %s, before blockStatus=%s",
                    nowBlockStatus.c_str(), blockStatus.second.c_str());

                int isEqual = nowBlockStatus.compare(blockStatus.second);

                if (isEqual != 0) {
                    LOG(INFO, "FileSystemImpl::evictBlock, is not equal, so cancel what have done before");

                    //4.2.1 END OF THE TRANSACTION
                    sharedMemoryManager->releaseLock();
                    //4.2.2 return to the original condition
                    noEvictBlockVector.push_back(tmpBlockID);

                    //4.2.3. delete the oss
                    qsReadWrite->qsDeleteObject((char *) fileNameInOSStmp.c_str());
                } else {

                    //4.3.5 rename the file in OSS
                    //BUG-FIX. should first rename the oss file, and then write the log.
                    //because. suppose this process is A and A is evict B's block.
                    // if A first write the log, then B can read the log, and knows that the file is in the oss
                    // however, the rename() operation have been completed. so when B read it from OSS. it will not see the file, so get failure.
                    qsReadWrite->renameObject((char *) fileNameInOSStmp.c_str(), (char *) fileNameInOSS.c_str());


                    //4.3.1 . write the previous file log
                    LOG(INFO, "FileSystemImpl::evictBlock, before blockIndex = %d, do really job", blockIndex);
                    std::vector<int32_t> previousVector;
                    previousVector.push_back(-blockIndex);
                    string previousRes = logFormat->serializeLog(previousVector, LogFormat::RecordType::remoteBlock);
                    writeFileStatusToLog((char *) previousFileName.c_str(), previousRes);

                    //4.3.2.set the shared memory 2->1;   4. change the file name
                    sharedMemoryManager->evictBlock(tmpBlockID, fileName);

                    //4.3.3  write the new file status to Log.
                    std::vector<int32_t> newVector;
                    newVector.push_back(tmpBlockID);
                    string res = logFormat->serializeLog(newVector, LogFormat::RecordType::evictBlock);
                    writeFileStatusToLog(fileName, res);


                    //4.3.4 BUG FIX.  we should replace the last block when we evict it to the OSS.
                    // this is in the same process
                    auto &status = fileStatusMap[fileName];
                    if ((std::strcmp(previousFileName.c_str(), fileName) == 0) &&
                        (tmpBlockID == status->getLastBucket())) {

                        int lastBucketIndex = getIndexAccordingBlockID(fileName, tmpBlockID);

                        //1. update the last bucket
                        status->setLastBucket(-(lastBucketIndex + 1));
                        status->setLastBucketIndex(lastBucketIndex);


                        LOG(INFO, "FileSystemImpl::evictBlock. change the last bucket ID, before = %d, after= %d.",
                            tmpBlockID, -(lastBucketIndex + 1));

                        //2. update the block vector
                        std::vector<int32_t> blockVector = status->getBlockIdVector();
                        blockVector[lastBucketIndex] = -(lastBucketIndex + 1);
                        status->setBlockIdVector(blockVector);
                    }




                    //4.3.6  END OF THE TRANSACTION

                    sharedMemoryManager->releaseLock();
                }

            }
            return noEvictBlockVector;
        }


        std::string FileSystemImpl::constructFileKey(std::string str, int blockIndex) {
            stringstream ss;
            string resVal;
            resVal.append(str.c_str(), strlen(str.c_str()));

            ss << resVal << "-" << blockIndex;
            return ss.str();
        }


        void FileSystemImpl::writeDate2OSS(char *fileNameInOSS, int blockID) {

            qsReadWrite->getPutObject(fileNameInOSS);

            LOG(INFO, "FileSystemImpl::writeDate2OSS, blockID=%d, fileNameInOSS=%s", blockID, fileNameInOSS);


            char *buffer = (char *) malloc(READ_BUFFER_SIZE * sizeof(char));
            const int iter = (int) (SIZE_OF_BLOCK / READ_BUFFER_SIZE);

            int64_t baseOffsetInBucket = blockID * SIZE_OF_BLOCK;


            int j;
            for (j = 0; j < iter; j++) {

                //MAYBE THIS IS A BUG. use lock to ensure the data we want to read is exactly the offset we have been seek
                sharedMemoryManager->getLock();
                fsSeek(baseOffsetInBucket, SEEK_SET);
                int64_t readBytes = readDataFromBucket(buffer, READ_BUFFER_SIZE);
                sharedMemoryManager->releaseLock();

                LOG(INFO, "FileSystemImpl::writeDate2OSS, readBytes = %d", readBytes);
                LOG(INFO, "FileSystemImpl::writeDate2OSS, read buffer = %s", buffer);
                if (readBytes <= 0) {
                    break;
                }
                qsReadWrite->qsWrite(fileNameInOSS, buffer, readBytes);
                baseOffsetInBucket += READ_BUFFER_SIZE;
            }

            //2. delete qingstor context
            qsReadWrite->closePutObject();
        }


        // start from 0
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


        void FileSystemImpl::getLock() {
            sharedMemoryManager->getLock();
        }

        void FileSystemImpl::releaseLock() {
            sharedMemoryManager->releaseLock();
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

        void FileSystemImpl::releaseOrInactiveLRUCache(char *fileName) {
            auto &status = fileStatusMap[fileName];
            std::vector<int32_t> lruBlockVector = status->getLruCache()->getAllKeyObject();
            if (lruBlockVector.size() > 0) {
                // BUG-FIX. in the lru cache. the block ID maybe empty or full. so should be checked first.
                std::vector<int32_t> emptyBlockVector;
                std::vector<int32_t> fullBlockVector;
                int lastBlockIndex = getIndexAccordingBlockID(fileName, status->getLastBucket());
                for (int j = 0; j < lruBlockVector.size(); j++) {
                    int tmpBlockID = lruBlockVector[j];
                    int index = getIndexAccordingBlockID(fileName, tmpBlockID);
                    if (index > lastBlockIndex) {
                        emptyBlockVector.push_back(tmpBlockID);
                        LOG(INFO,
                            "FileSystemImpl::closeFile. the evict block is empty, and the blockID =%d, lastBlock =%d.",
                            tmpBlockID, status->getLastBucket());
                    } else {
                        fullBlockVector.push_back(tmpBlockID);
                        LOG(INFO,
                            "FileSystemImpl::closeFile. the evict block is full, and the blockID =%d, lastBlock =%d.",
                            tmpBlockID, status->getLastBucket());
                    }
                }

                // inactive the full block ID
                inactiveBlock(fileName, fullBlockVector);

                //release the empty block
                releaseBlock(fileName, emptyBlockVector);
            }
        }


        //TODO, flush, write log and so on
        void FileSystemImpl::closeFile(char *fileName) {

            // 1. get the full lru cache, inactive it or release it
            releaseOrInactiveLRUCache(fileName);


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

            auto &status = fileStatusMap[fileName];
            rebuildFileStatus->setEndOffsetOfBucket(status->getEndOffsetOfBucket());

            LOG(INFO, "FileSystemImpl::closeFile out read length = %d", length);

            //5. write the close status to log
            std::string res = logFormat->serializeFileStatusForClose(rebuildFileStatus);
            ftruncate(logFd, 0);
            lseek(logFd, 0, SEEK_SET);
            int writeSize = write(logFd, res.data(), res.size());
            close(logFd);
        }

        // delete the file status's log
        void FileSystemImpl::deleteFile(char *fileName) {
            // 1. get the full lru cache, inactive it or release it
            releaseOrInactiveLRUCache(fileName);


            char *filePathName = getFilePath(fileName);
            LOG(INFO, "FileSystemImpl::deleteFile. delete the file =%s", fileName);
            remove(filePathName);

            //**************TODO JUST FOR TEST, NEET TO BE DELETED********************************
            LOG(INFO, "FileSystemImpl::deleteFile. 1. in the end the  shared memory status is :");
            sharedMemoryManager->printSMStatus();
            LOG(INFO, "FileSystemImpl::deleteFile. 2. in the end the  shared memory status is :");
            //**************TODO JUST FOR TEST, NEET TO BE DELETED********************************

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

            //1. set the file name
            fileStatus->setFileName(fileName);

            //2. set the last bucket
            int blockVectorSize = fileStatus->getBlockIdVector().size();
            if (blockVectorSize > 0) {
                fileStatus->setLastBucket(fileStatus->getBlockIdVector()[blockVectorSize - 1]);
                fileStatus->setLastBucketIndex(blockVectorSize - 1);
            }

            LOG(INFO,
                "*********************** in the end, before the close File Status*********************************");
            int64_t endOffsetOfBucket = fileStatus->getEndOffsetOfBucket();
            vector<int32_t> blockIDVector = fileStatus->getBlockIdVector();
            LOG(INFO,
                "FileSystemImpl::readCloseFileStatus. the lastBucket=%d, endOffsetOfBucket=%d,blockIDVector.size=  %d",
                fileStatus->getLastBucket(), endOffsetOfBucket, blockIDVector.size());
            for (int i = 0; i < blockIDVector.size(); i++) {
                LOG(INFO, " block id = %d", blockIDVector[i]);
            }

            LOG(INFO,
                "*********************** in the end, after the close File Status*********************************");
            close(logFd);


//            read data from the block id lists without change the cache
            readTotalDataFromFile(fileStatus);


            //read random data from file without change the cache.
//            readTotalRandomDataFromFile(fileStatus);

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
        void FileSystemImpl::readDataFromFileAccordingToBlockID(int blockID, std::shared_ptr<FileStatus> fileStatus,
                                                                string suffixName) {
            int SIZE = 128;
            char *readBuf = new char[SIZE];
            int iter = SIZE_OF_BLOCK / SIZE;
            std::string fileName = fileStatus->getFileName();

            stringstream ss;
            ss << "/ssdfile/ssdkv/" << fileName << "-" << suffixName;
            string filePath = ss.str();

            vector<int32_t> blockIDVector = fileStatus->getBlockIdVector();
            LOG(INFO, "1. FileSystemImpl::readDataFromFileAccordingToBlockID. filePath=%s. block vector size = %d",
                filePath.c_str(),
                blockIDVector.size());

            if (blockID == fileStatus->getLastBucket()) {
                LOG(INFO, "3. FileSystemImpl::readDataFromFileAccordingToBlockID. come in if");

                int64_t endOfBucketOffset = fileStatus->getEndOffsetOfBucket();

                if (blockID >= 0) {

                    int64_t readOffset = blockID * SIZE_OF_BLOCK;
                    while (endOfBucketOffset > 0) {
                        int64_t leftData = SIZE <= endOfBucketOffset ? SIZE : endOfBucketOffset;
                        LOG(INFO,
                            "4. FileSystemImpl::readDataFromFileAccordingToBlockID. come in if. leftData=%d,endOfBucketOffset=%d",
                            leftData, endOfBucketOffset);

                        sharedMemoryManager->getLock();
                        fsSeek(readOffset, SEEK_SET);
                        int64_t readData = readDataFromBucket(readBuf, leftData);
                        sharedMemoryManager->releaseLock();

                        writeCharStrUtil(filePath, readBuf, readData);
                        readBuf = new char[SIZE];
                        endOfBucketOffset -= readData;
                        readOffset += readData;

                    }

                } else {
                    std::string ossFileName = constructFileKey(fileName, -blockID);
                    LOG(INFO,
                        "5. FileSystemImpl::readDataFromFileAccordingToBlockID. come in if. ossFileName = %s",
                        ossFileName.c_str());

                    qsReadWrite->getGetObject((char *) ossFileName.data());
                    while (endOfBucketOffset > 0) {
                        int64_t leftData = SIZE <= endOfBucketOffset ? SIZE : endOfBucketOffset;
                        LOG(INFO,
                            "5. FileSystemImpl::readDataFromFileAccordingToBlockID. come in if. leftData=%d,endOfBucketOffset=%d",
                            leftData, endOfBucketOffset);

                        int64_t readData = qsReadWrite->qsRead((char *) ossFileName.c_str(), readBuf, leftData);
                        writeCharStrUtil(filePath, readBuf, readData);
                        readBuf = new char[SIZE];
                        endOfBucketOffset -= readData;
                    }

                    qsReadWrite->closeGetObject();
                }

            } else {
                LOG(INFO, "FileSystemImpl::readDataFromFileAccordingToBlockID. come in else");
                if (blockID >= 0) {
                    int64_t readOffset = blockID * SIZE_OF_BLOCK;

                    for (int j = 0; j < iter; j++) {

                        sharedMemoryManager->getLock();
                        fsSeek(readOffset, SEEK_SET);
                        int64_t readData = readDataFromBucket(readBuf, SIZE);
                        sharedMemoryManager->releaseLock();


                        LOG(INFO, "7. FileSystemImpl::readDataFromFileAccordingToBlockID. come in if. j=%d, readBuf=%s",
                            j,
                            readBuf);
                        writeCharStrUtil(filePath, readBuf, SIZE);
                        readBuf = new char[SIZE];
                        readOffset += readData;
                    }
                } else {
                    std::string ossFileName = constructFileKey(fileName, -blockID);
                    qsReadWrite->getGetObject((char *) ossFileName.data());
                    for (int j = 0; j < iter; j++) {
                        LOG(INFO,
                            "8. FileSystemImpl::readDataFromFileAccordingToBlockID. come in if. j=%d, ossFileName=%s",
                            j,
                            ossFileName.c_str());
                        qsReadWrite->qsRead((char *) ossFileName.c_str(), readBuf, SIZE);
                        LOG(INFO, "8. FileSystemImpl::readDataFromFileAccordingToBlockID. readBuf=%s", readBuf);
                        writeCharStrUtil(filePath, readBuf, SIZE);
                        readBuf = new char[SIZE];
                    }
                    qsReadWrite->closeGetObject();
                }
            }
        }


        //TODO ,JUST FOR TEST
        void FileSystemImpl::readTotalDataFromFile(std::shared_ptr<FileStatus> fileStatus) {
            vector<int32_t> blockIDVector = fileStatus->getBlockIdVector();
            string suffixName = "readTotalDataFromFile";
            for (int i = 0; i < blockIDVector.size(); i++) {
                int blockID = blockIDVector[i];
                readDataFromFileAccordingToBlockID(blockID, fileStatus, suffixName);
            }
        }

        //TODO ,JUST FOR TEST
        void FileSystemImpl::readTotalRandomDataFromFile(std::shared_ptr<FileStatus> fileStatus) {
            vector<int32_t> blockIDVector = fileStatus->getBlockIdVector();
            string suffixName = "readTotalRandomData";
            int start = 0;
            int end = blockIDVector.size() - 1;


            //1. read data and write the result to file
            vector<int32_t> randomBlockIDVector;
            vector<int32_t> randomIndexVector;
            for (int i = 0; i < blockIDVector.size(); i++) {
                int randomIndex = getRandomIntValue(start, end);
                randomIndexVector.push_back(randomIndex);
                int randomBlockID = blockIDVector[randomIndex];
                randomBlockIDVector.push_back(randomBlockID);
                readDataFromFileAccordingToBlockID(randomBlockID, fileStatus, suffixName);
            }

            //2. write the random blockID vector to file to check the read is right or not
            stringstream ss;
            ss << "/ssdfile/ssdkv/" << fileStatus->getFileName() << "-randomBlockID";
            string filePath = ss.str();

            writeIntArrayUtil(filePath, randomBlockIDVector);


            //2. write the random index vector to file to check the read is right or not
            stringstream ss2;
            ss2 << "/ssdfile/ssdkv/" << fileStatus->getFileName() << "-randomIndex";
            string filePathForIndex = ss2.str();

            writeIntArrayUtil(filePathForIndex, randomIndexVector);

            //3. read the verify file status to check the random read is right or not?
            readTotalRandomDataFromVerifyFile(randomIndexVector, fileStatus);
        }

        //TODO ,JUST FOR TEST
        void FileSystemImpl::readTotalRandomDataFromVerifyFile(vector<int32_t> randomIndexVector,
                                                               std::shared_ptr<FileStatus> fileStatus) {
            stringstream ss;
            ss << "/ssdfile/ssdkv/" << fileStatus->getFileName() << "-readTotalDataFromFile";
            string fileTobeRead = ss.str();

            LOG(INFO, "FileSystemImpl::readTotalRandomDataFromVerifyFile fileTobeRead=%s", fileTobeRead.c_str());

            stringstream ss2;
            ss2 << "/ssdfile/ssdkv/" << fileStatus->getFileName() << "-readTotalRandomDataFromVerifyFile";
            string fileTobeWrite = ss2.str();

            LOG(INFO, "FileSystemImpl::readTotalRandomDataFromVerifyFile fileTobeWrite=%s", fileTobeWrite.c_str());

            //read data from the verify file
            int flags = O_CREAT | O_RDWR;
            int verifyFd = open(fileTobeRead.c_str(), flags, 0644);
            int SIZE = 128;
            char *readBuf = new char[SIZE];
            int iter = SIZE_OF_BLOCK / SIZE;

            for (int i = 0; i < randomIndexVector.size(); i++) {
                int64_t offset = randomIndexVector[i] * 1024;
                int blockID = fileStatus->getBlockIdVector()[i];
                if (blockID == fileStatus->getLastBucket()) {
                    int64_t res = lseek(verifyFd, offset, SEEK_SET);
                    int64_t endOfBucketOffset = fileStatus->getEndOffsetOfBucket();
                    while (endOfBucketOffset > 0) {
                        int64_t leftData = SIZE <= endOfBucketOffset ? SIZE : endOfBucketOffset;
                        LOG(INFO,
                            "4. FileSystemImpl::readTotalRandomDataFromVerifyFile. come in if. leftData=%d,endOfBucketOffset=%d",
                            leftData, endOfBucketOffset);

                        int64_t readData = read(verifyFd, readBuf, SIZE);
                        writeCharStrUtil(fileTobeWrite, readBuf, readData);
                        readBuf = new char[SIZE];
                        endOfBucketOffset -= readData;
                    }
                } else {
                    int64_t res = lseek(verifyFd, offset, SEEK_SET);
                    for (int j = 0; j < iter; j++) {
                        int64_t readData = read(verifyFd, readBuf, SIZE);
                        LOG(INFO, "7. FileSystemImpl::readTotalRandomDataFromVerifyFile. come in if. j=%d, readBuf=%s",
                            j,
                            readBuf);
                        writeCharStrUtil(fileTobeWrite, readBuf, readData);
                        readBuf = new char[SIZE];
                    }
                }
            }
        }


        /**
         *
         * @param start
         * @param end
         * @return the int value between [start,end]
         */
        int FileSystemImpl::getRandomIntValue(int start, int end) {
            int val = (rand() % (end - start + 1)) + start;
            return val;
        }


        //TODO ,JUST FOR TEST
        void FileSystemImpl::writeCharStrUtil(string fileName, char *buf, int64_t size) {
            std::ofstream ostrm(fileName, std::ios::out | std::ios::app);
            LOG(INFO, "FileSystemImpl::writeCharStrUtil .fileName=%s,  buf=%s", fileName.c_str(), buf);
            ostrm.write(buf, size);
        }

        //TODO ,JUST FOR TEST
        void FileSystemImpl::writeIntArrayUtil(string fileName, vector<int32_t> randomVector) {
            std::ofstream ostrm(fileName, std::ios::out | std::ios::app);
            LOG(INFO, "FileSystemImpl::writeIntArrayUtil .fileName=%s", fileName.c_str());
            for (int i = 0; i < randomVector.size(); i++) {
                ostrm << randomVector[i];
                if (i != randomVector.size() - 1) {
                    ostrm << ",";
                }
            }
        }


    }

}