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
                int64_t remainOffsetInBlock = status->getEndOffsetOfBucket() - cursorOffset;
                int64_t bufLength = size < remainOffsetInBlock ? size : remainOffsetInBlock;
                int64_t readLength = filesystem->readDataFromBucket(buf, bufLength);
                cursorOffset += readLength;
                return readLength;
            } else {
                LOG(INFO, "InputStreamImpl::readInternal. cursorBucketID=%d, cursorOffset=%d", cursorBucketID,
                    cursorOffset);
                int64_t remainOffsetInBlock = SIZE_OF_BLOCK - cursorOffset;
                if (size <= remainOffsetInBlock) {
                    int64_t readLength = filesystem->readDataFromBucket(buf, size);
//                LOG(INFO, "1. InputStreamImpl::readInternal. readLength=%d", readLength);
                    cursorOffset += readLength;
                    return readLength;
                } else {
                    int64_t remainOffsetTotal = getRemainLength();

                    int64_t bufLength = size < remainOffsetTotal ? size : remainOffsetTotal;

                    char resBuf[bufLength];

                    char tmpBuf[SIZE_OF_BLOCK];

                    int64_t sizeRemain = bufLength;
                    int64_t bufOffset = 0;

                    //read the first block data of the file.
                    int readLength = filesystem->readDataFromBucket(tmpBuf, remainOffsetInBlock);
//                LOG(INFO, "2. InputStreamImpl::readInternal. readLength=%d", readLength);
                    sizeRemain -= readLength;
                    memcpy(resBuf + bufOffset, tmpBuf, readLength);
                    bufOffset += readLength;

                    while (sizeRemain > 0) {
                        if (sizeRemain > SIZE_OF_BLOCK) {
                            seekToNextBlock();
                            int64_t readLength = filesystem->readDataFromBucket(tmpBuf, SIZE_OF_BLOCK);
//                        LOG(INFO, "3. InputStreamImpl::readInternal. readLength=%d", readLength);
                            sizeRemain -= SIZE_OF_BLOCK;
                            memcpy(resBuf + bufOffset, tmpBuf, readLength);
                            bufOffset += readLength;

                            cursorOffset = readLength;
                        } else {
                            seekToNextBlock();
                            int64_t readLength = filesystem->readDataFromBucket(tmpBuf, sizeRemain);
//                        LOG(INFO, "4. InputStreamImpl::readInternal. readLength=%d", readLength);
                            sizeRemain -= sizeRemain;
                            memcpy(resBuf + bufOffset, tmpBuf, readLength);
                            bufOffset += readLength;

                            cursorOffset = readLength;
                        }
                    }
                    memcpy(buf, resBuf, sizeof(resBuf));
                    return bufOffset;
                }
            }
        }


        void InputStreamImpl::seekToNextBlock() {
            this->cursorIndex++;

            //check the pos is in the oss or not
            checkStatus(cursorIndex * SIZE_OF_BLOCK);

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
            if (theEOFOffset == 0) {
                LOG(INFO, "the file do not contain any one bucket");
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
            int i = 0;
            for (i = 0; i < status->getPingIDVector().size(); i++) {
                if (status->getPingIDVector()[i] == blockID) {
                    break;
                }
            }

            /*********************************DOTO FOR TEST********************/

            for (i = 0; i < status->getPingIDVector().size(); i++) {
                LOG(INFO, "InputStreamImpl::checkStatus. status->getPingIDVector()[i]=%d",
                    status->getPingIDVector()[i]);
            }

            /*********************************DOTO FOR TEST********************/

            //3.1 see the block is in the SSD bucket or in the OSS.
            if (i >= status->getPingIDVector().size()) {
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