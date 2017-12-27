//
// Created by neuyilan@163.com on 11/18/17.
//


#include "InputStreamImpl.h"

using namespace std;
namespace Gopherwood {
    namespace Internal {
        InputStreamImpl::InputStreamImpl(std::shared_ptr<FileSystemInter> fs, const char *fileName,
                                         bool verifyChecksum) {
            LOG(INFO, "InputStreamImpl method");
            this->filesystem = fs;
            this->fileName = fileName;
            //1.check the file exist in the fileStatusMap or not
            bool exist = filesystem->checkFileExist((char *) fileName);
            if (!exist) {
                LOG(INFO, "fileStatusMap do not contain the file, so rebuild the fileStatusMap and find again");
                //1.1. rebuild fileStatusMap from log .
                filesystem->rebuildFileStatusFromLog((char *) fileName);
                //1.2 check again
                bool existAgain = filesystem->checkFileExist((char *) fileName);
                if (!existAgain) {
                    LOG(INFO, "open an file not found, please check the file exist or not");
                    return;
                }
            }
            status = filesystem->getFileStatus(fileName);
            //3. default seek the offset to zero when read.
            seek(0);
        }

        InputStreamImpl::~InputStreamImpl() {

        }


//        void InputStreamImpl::open(std::shared_ptr<FileSystemInter> fs, const char *fileName, bool verifyChecksum) {
//            status = fs->rebuildFileStatusFromLog(fileName);
//
//            bool exist = status->isExist();
//            if (!exist) {
//                cout << "open an file not found, please check the file exist or not?" << endl;
//                return ;
//            }
//
//            closed = false;
//        }


        //TODO should do chase from log first
        int32_t InputStreamImpl::read(char *buf, int32_t size) {
            try {
                int32_t done = readInternal(buf, size);
                return done;
            } catch (...) {
                throw;
            }
        }


        int32_t InputStreamImpl::readInternal(char *buf, int32_t size) {
            if (status->getLastBucket() == cursorBucketID) {
                if (cursorOffset > status->getEndOffsetOfBucket()) {
                    LOG(LOG_ERROR, "InputStreamImpl::readInternal, read offset exceed the size of the file");
                    return -1;
                }
                int64_t remainOffsetInBlock = status->getEndOffsetOfBucket() - cursorOffset;
                int64_t bufLength = size < remainOffsetInBlock ? size : remainOffsetInBlock;
                int64_t readLength = filesystem->readDataFromBucket(buf, bufLength);
                cursorOffset += readLength;
                LOG(INFO, "InputStreamImpl::readInternal. 1.&&&&&&&&&&&&&&&&&&&&&&&&&&&& buf=%s", buf);
                LOG(INFO,
                    "InputStreamImpl::readInternal. 1.readLength = %d,remainOffsetInBlock=%d,endOffsetOfBucket=%d, cursorOffset=%d",
                    readLength, remainOffsetInBlock, status->getEndOffsetOfBucket(), cursorOffset);

                if (cursorOffset >= SIZE_OF_BLOCK) {
                    seekToNextBlock();
                }
                return readLength;
            } else {
                LOG(INFO, "InputStreamImpl::readInternal. cursorBucketID=%d, cursorOffset=%d", cursorBucketID,
                    cursorOffset);
                int64_t remainOffsetInBlock = SIZE_OF_BLOCK - cursorOffset;
                if (size <= remainOffsetInBlock) {
                    int64_t readLength = filesystem->readDataFromBucket(buf, size);
//                LOG(INFO, "1. InputStreamImpl::readInternal. readLength=%d", readLength);
                    cursorOffset += readLength;
                    LOG(INFO, "InputStreamImpl::readInternal. 2.&&&&&&&&&&&&&&&&&&&&&&&&&&&& buf=%s", buf);
                    LOG(INFO, "InputStreamImpl::readInternal. 2.readLength = %d", readLength);

                    if (cursorOffset >= SIZE_OF_BLOCK) {
                        seekToNextBlock();
                    }
                    return readLength;
                } else {
                    int64_t remainOffsetTotal = getRemainLength();

                    int64_t bufLength = size < remainOffsetTotal ? size : remainOffsetTotal;

                    //1. read the remain data in the block
                    int64_t readLength = filesystem->readDataFromBucket(buf, remainOffsetInBlock);
                    int totalOffset = 0;
                    totalOffset += readLength;
                    bufLength -= readLength;

                    while (bufLength > 0) {
                        if (bufLength > SIZE_OF_BLOCK) {
                            seekToNextBlock();
                            readLength = filesystem->readDataFromBucket(buf + totalOffset, SIZE_OF_BLOCK);
                            bufLength -= readLength;
                            totalOffset += readLength;
                            cursorOffset += readLength;
                        } else {
                            seekToNextBlock();
                            readLength = filesystem->readDataFromBucket(buf + totalOffset, bufLength);
                            bufLength -= readLength;
                            totalOffset += readLength;
                            cursorOffset += readLength;
                        }
                    }
                    LOG(INFO, "InputStreamImpl::readInternal. 3.&&&&&&&&&&&&&&&&&&&&&&&&&&&& buf=%s", buf);
                    if (cursorOffset >= SIZE_OF_BLOCK) {
                        seekToNextBlock();
                    }
                    return totalOffset;
                }
            }
        }


        void InputStreamImpl::seekToNextBlock() {
            this->cursorIndex++;

            //check the pos is in the oss or not
            checkStatus(cursorIndex * SIZE_OF_BLOCK);

            LOG(INFO, "InputStreamImpl::seekToNextBlock, status->getBlockIdVector().size()=%d, cursorIndex=%d",
                status->getBlockIdVector().size(), cursorIndex);

            if (cursorIndex >= status->getBlockIdVector().size()) {
                return;
            }
            this->cursorBucketID = status->getBlockIdVector()[cursorIndex];
            this->cursorOffset = 0;
            //seek the offset of the bucket file
            this->filesystem->fsSeek(cursorBucketID * SIZE_OF_BLOCK + cursorOffset, SEEK_SET);
        }


        //return the remain length from the cursorOffset to the endOfFile.
        //cursorOffset<------------>endOfFile
        int64_t InputStreamImpl::getRemainLength() {
            int64_t remainLength = 0;
            int i = cursorIndex;
            remainLength += SIZE_OF_BLOCK - cursorOffset;
            i++;
            for (; i < status->getBlockIdVector().size(); i++) {
                remainLength += SIZE_OF_BLOCK;
            }
            return remainLength;
        }


        void InputStreamImpl::readFully(char *buf, int64_t size) {

        }


        int64_t InputStreamImpl::available() {

        }


        void InputStreamImpl::seek(int64_t pos) {
            checkStatus(pos);
            try {
                seekInternal(pos);
            } catch (...) {
                throw;
            }

        }

        void InputStreamImpl::seekInternal(int64_t pos) {

            int32_t bucketIDIndex = pos / SIZE_OF_BLOCK;
            int64_t bucketOffset = pos % SIZE_OF_BLOCK;
            //TODO, should this in the FileSystem or in the InputStream?
            this->status = filesystem->getFileStatus(fileName.data());
            this->cursorIndex = bucketIDIndex;
            LOG(INFO, "InputStreamImpl::seekInternal cursorIndex = %d", cursorIndex);
            if (status->getBlockIdVector().size() <= cursorIndex) {
                LOG(LOG_ERROR, "InputStreamImpl::seekInternal, cursorIndex smaller than the block id's size");
                return;
            }
            this->cursorBucketID = status->getBlockIdVector()[cursorIndex];
            LOG(INFO, "InputStreamImpl::seekInternal cursorBucketID = %d", cursorBucketID);

            this->cursorOffset = bucketOffset;

            //seek the offset of the bucket file
            this->filesystem->fsSeek(cursorBucketID * SIZE_OF_BLOCK + cursorOffset, SEEK_SET);
        }


        int64_t InputStreamImpl::tell() {

        }


        //TODO,
        void InputStreamImpl::close() {
            filesystem->closeFile((char *) fileName.data());
        }


        string InputStreamImpl::toString() {

        }

        void InputStreamImpl::checkStatus(int64_t pos) {
            LOG(INFO, "InputStreamImpl::checkStatus pos = %d", pos);
            if (pos < 0) {
                LOG(LOG_ERROR, "InputStreamImpl::checkStatus pos can not be smaller than zero");
            }
            //1. check the size of the file
            int64_t theEOFOffset = this->filesystem->getTheEOFOffset(this->fileName.data());
            LOG(INFO, "InputStreamImpl::checkStatus. theEOFOffset=%d", theEOFOffset);

            if (theEOFOffset == 0) {
                LOG(INFO, "the file do not contain any one bucket");
                return;
            }
            if (status->getBlockIdVector().size() == 0) {
                LOG(LOG_ERROR, "InputStreamImpl::checkStatus. do not contain any file");
                return;
            }

            if (pos > theEOFOffset) {
                //todo, throw error
                LOG(LOG_ERROR, "error, the given pos exceed the size of the file");
                return;
            }

            //2. get the blockID which seeks to
            int64_t blockIndex = pos / SIZE_OF_BLOCK;
            int blockID = status->getBlockIdVector()[blockIndex];

            LOG(INFO, "InputStreamImpl::checkStatus. blockID=%d", blockID);

            //3. check the blockID is PING or not.(its type='1' or not)
            int index = 0;
            for (index = 0; index < status->getPingIDVector().size(); index++) {
                if (status->getPingIDVector()[index] == blockID) {
                    break;
                }
            }

            /*********************************DOTO FOR TEST********************/
            for (int j = 0; j < status->getPingIDVector().size(); j++) {
                LOG(INFO, "InputStreamImpl::checkStatus. status->getPingIDVector()[i]=%d",
                    status->getPingIDVector()[j]);
            }
            /*********************************DOTO FOR TEST********************/

            //3.1 see the block is in the SSD bucket or in the OSS.
            if (index >= status->getPingIDVector().size()) {
                if (blockID >= 0) {
                    //3.1.1 the block is in the SSD bucket
                    bool isEqual = filesystem->checkBlockIDWithFileName(blockID, fileName);
                    if (isEqual) {
                        LOG(INFO, "3.1.1. InputStreamImpl::checkStatus the block is in the SSD bucket");
                        vector<int32_t> newPingBlockVector;
                        newPingBlockVector.push_back(blockID);
                        filesystem->checkAndAddPingBlockID((char *) fileName.data(), newPingBlockVector);
                        for (int i = 0; i < newPingBlockVector.size(); i++) {
                            filesystem->changePingBlockActive(newPingBlockVector[i]);
                        }
                        return;
                    } else {
                        //3.1.2 the block is in the OSS
                        LOG(INFO, "3.1.2. InputStreamImpl::checkStatus the block is in the OSS");
                        filesystem->catchUpFileStatusFromLog((char *) fileName.data(), status->getLogOffset());
                        filesystem->writeDataFromOSS2Bucket(blockIndex, fileName);
                    }
                } else {
                    //3.2.the block is in the OSS
                    filesystem->writeDataFromOSS2Bucket(blockIndex, fileName);
                    LOG(INFO, "3.2. InputStreamImpl::checkStatus the block is in OSS");
                }
            } else {
                LOG(INFO, "2. InputStreamImpl::checkStatus see the block is in the SSD bucket ");
            }
        }

    }
}