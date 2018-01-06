
#include "OutputStreamImpl.h"
#include "../common/Logger.h"

namespace Gopherwood {
    namespace Internal {


        OutputStreamImpl::OutputStreamImpl(std::shared_ptr<FileSystemInter> fs, char *fileName, int flag) {

            this->filesystem = fs;
            this->fileName = fileName;

            //1.check the file exist in the fileStatusMap or not
            bool exist = filesystem->checkFileExist(fileName);
            LOG(INFO, "exist =  %d", exist);

            if (!exist) {
                LOG(INFO,
                    "OutputStreamImpl::OutputStreamImpl. fileStatusMap do not contain the file, so rebuild the fileStatusMap and find again");
                //1.1. rebuild fileStatusMap from log .
                filesystem->rebuildFileStatusFromLog(fileName);
                //1.2 check again
                bool existAgain = filesystem->checkFileExist(fileName);
                if (!existAgain) {
                    LOG(INFO, "no file named = %s, so create new one ", fileName);
                    //1.3 create new one
                    filesystem->createFile(fileName);

                    //TODO 1.4 sync new file status to log

                }
            } else {
                //2. the fileName exist in the fileStatusMap, so catch up the fileStatus from LOG.
                //TODO ,we should mark the offset that last time catch.
            }
            status = filesystem->getFileStatus(fileName);
            //3. default seek to the end of the file
            LOG(INFO, "getBlockIdVector().size() = %d,status->getLastBucket() = %d, status->getEndOffsetOfBucket()=%d",
                status->getBlockIdVector().size(), status->getLastBucket(), status->getEndOffsetOfBucket());
            int64_t endOfOffset = filesystem->getTheEOFOffset(fileName);
            LOG(INFO, "endOfOffset = %d", endOfOffset);

            if (status->getBlockIdVector().size() == 0) {
                LOG(INFO, "openFile do not seek, because the file do not contain any bucket");
                //do nothing, because the file do not contain any bucket.
            } else {
                seek(endOfOffset);
            }

        }

        OutputStreamImpl::~OutputStreamImpl() {

        }

        void OutputStreamImpl::write(const char *buf, int64_t size) {
//            LOG(INFO, "come in the write method in OutputStreamImpl");
            if (NULL == buf || size < 0) {
                LOG(INFO, "Invalid parameter.");
//                THROW(InvalidParameter, "Invalid parameter.");
            }
            if (status->getBlockIdVector().size() == 0) {
                seek(0);
            }
            try {
                if (status->getBlockIdVector().size() == 0) {
                    return;
                }
                writeInternal((char *) buf, size);
            } catch (...) {
                setError(current_exception());
                throw;
            }
        }


        void OutputStreamImpl::writeInternal(char *buf, int64_t size) {
            LOG(INFO, "come in the writeInternal method in OutputStreamImpl");
            int64_t remainOffsetInBlock = SIZE_OF_BLOCK - cursorOffset;
            LOG(INFO, "cursorOffset = %d, cursorIndex=%d,cursorBucketID=%d", cursorOffset, cursorIndex, cursorBucketID);
            LOG(INFO, "data write size = %d, remainOffsetInBlock=%d", size, remainOffsetInBlock);
            if (size <= remainOffsetInBlock) {
                filesystem->fsSeek(cursorBucketID * SIZE_OF_BLOCK + cursorOffset, SEEK_SET);
                filesystem->writeDataToBucket(buf, size);
                cursorOffset += size;
            } else {
                int64_t remainOffsetTotal = getRemainLength();

                //TODO, this maybe wrong, because, I don't know when acquire a block, it will sync with the filesystem/FileStatus or not.
                //TODO, if it does not work, we should check the code and try another method.
                while ((size > remainOffsetTotal)) {
                    LOG(INFO, "OutputStreamImpl::writeInternal. 2. remainOffsetTotal before =%d", remainOffsetTotal);
                    //1&2 acquire one block, sync to the shared memory and LOG system.
                    filesystem->acquireNewBlock((char *) fileName.data());
                    int64_t afterRemainOffsetTotal = getRemainLength();

                    remainOffsetTotal = afterRemainOffsetTotal;
                    LOG(INFO, "OutputStreamImpl::writeInternal. 2. remainOffsetTotal after =%d", remainOffsetTotal);
                }

                //1. BUG-FIX, IMPORTANT. because, when above code execute, especially the acquireNewBlock method execute,
                // it will change the ssd bucket's offset.
                filesystem->fsSeek(cursorBucketID * SIZE_OF_BLOCK + cursorOffset, SEEK_SET);
                //2. write remainOffsetInBlock data to the bucket first;
                filesystem->writeDataToBucket(buf, remainOffsetInBlock);
                int64_t haveWriteSize = remainOffsetInBlock;
                int64_t remainSize = size - haveWriteSize;

                while (remainSize > 0) {
                    if (remainSize > SIZE_OF_BLOCK) {
                        seekToNextBlock();
                        filesystem->writeDataToBucket(buf + haveWriteSize, SIZE_OF_BLOCK);
                        cursorOffset = SIZE_OF_BLOCK;
                        remainSize -= SIZE_OF_BLOCK;
                        haveWriteSize += SIZE_OF_BLOCK;
                    } else {
                        seekToNextBlock();
                        filesystem->writeDataToBucket(buf + haveWriteSize, remainSize);
                        cursorOffset = remainSize;
                        remainSize -= remainSize;
                        haveWriteSize += remainSize;
                    }
                }
            }

            //3. set the endOffsetOfBucket
            status->setEndOffsetOfBucket(cursorOffset);
            LOG(INFO, "cursorOffset=%d", cursorOffset);

            //4. TODO. write new file status to the LOG system.
        }


        int64_t OutputStreamImpl::getRemainLength() {
            int64_t remainLength = 0;

            int i = cursorIndex;
            LOG(INFO, "OutputStreamImpl::getRemainLength. i = %d, status->getBlockIdVector().size=%d", i,
                status->getBlockIdVector().size());
            remainLength += SIZE_OF_BLOCK - cursorOffset;
            i++;
            for (; i < status->getBlockIdVector().size(); i++) {
                remainLength += SIZE_OF_BLOCK;
                LOG(INFO, "OutputStreamImpl::getRemainLength************** come in here");
            }

            LOG(INFO, "OutputStreamImpl::remainLength=%d", remainLength);
            return remainLength;

        }


        void OutputStreamImpl::flush() {

        }


        int64_t OutputStreamImpl::tell() {

        }


        void OutputStreamImpl::sync() {

        }


        void OutputStreamImpl::close() {
            filesystem->closeFile((char *) fileName.data());
        }

        void OutputStreamImpl::seek(int64_t pos) {
            checkStatus(pos);
            try {
                seekInternal(pos);
            } catch (...) {
                throw;
            }
        }


        void OutputStreamImpl::seekInternal(int64_t pos) {
            if (status->getBlockIdVector().size() <= 0) {
                LOG(LOG_ERROR, "the block vector size is less than zero");
                return;
            }

            int64_t theEOFOffset = this->filesystem->getTheEOFOffset(this->fileName.data());
            LOG(INFO, "OutputStreamImpl::seekInternal, theEOFOffset = %d", theEOFOffset);

            if (pos > theEOFOffset) {
                //todo, throw error
                LOG(LOG_ERROR, "error, the given pos exceed the size of the file");
            }
            int32_t bucketIDIndex = pos / SIZE_OF_BLOCK;
            int64_t bucketOffset = pos % SIZE_OF_BLOCK;

            //TODO, should this in the FileSystem or in the InputStream?
            this->status = filesystem->getFileStatus(fileName.data());
            this->cursorIndex = bucketIDIndex;
            LOG(INFO, "OutputStreamImpl cursorIndex = %d", cursorIndex);

            this->cursorBucketID = status->getBlockIdVector()[cursorIndex];
            LOG(INFO, "OutputStreamImpl cursorBucketID = %d", cursorBucketID);
            this->cursorOffset = bucketOffset;

            //seek the offset of the bucket file
            this->filesystem->fsSeek(cursorBucketID * SIZE_OF_BLOCK + cursorOffset, SEEK_SET);
        }


        string OutputStreamImpl::toString() {

        }

        void OutputStreamImpl::checkStatus(int64_t pos) {
            if (status->getBlockIdVector().size() == 0) {
                LOG(INFO, "checkStatus, the file do not contain any bucket, so create new one for write");
                filesystem->acquireNewBlock((char *) fileName.data());
            }


            LOG(INFO, "OutputStreamImpl::checkStatus pos = %d", pos);
            if (pos < 0) {
                LOG(LOG_ERROR, "OutputStreamImpl::checkStatus pos can not be smaller than zero");
            }
            //1. check the size of the file
            int64_t theEOFOffset = this->filesystem->getTheEOFOffset(this->fileName.data());
            LOG(INFO, "OutputStreamImpl::checkStatus. theEOFOffset=%d", theEOFOffset);

            if (theEOFOffset == 0) {
                LOG(INFO, "the file do not contain any one bucket");
                return;
            }
            if (status->getBlockIdVector().size() == 0) {
                LOG(LOG_ERROR, "OutputStreamImpl::checkStatus. do not contain any file");
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

            LOG(INFO, "OutputStreamImpl::checkStatus. blockID=%d", blockID);

            //3. check the blockID is PING or not.(its type='1' or not)
            int index = 0;
            for (index = 0; index < status->getPingIDVector().size(); index++) {
                if (status->getPingIDVector()[index] == blockID) {
                    return;
                }
            }

            /*********************************todo FOR TEST********************/
            for (int j = 0; j < status->getPingIDVector().size(); j++) {
                LOG(INFO, "OutputStreamImpl::checkStatus. status->getPingIDVector()[i]=%d",
                    status->getPingIDVector()[j]);
            }
            /*********************************DOTO FOR TEST********************/

            //3.1 see the block is in the SSD bucket or in the OSS.
            if (index >= status->getPingIDVector().size()) {
                if (blockID >= 0) {
                    //3.1.1 the block is in the SSD bucket
                    bool isEqual = filesystem->checkBlockIDWithFileName(blockID, fileName);

                    LOG(INFO, "OutputStreamImpl::checkStatus isEqual=%d", isEqual);
                    if (isEqual) {
                        LOG(INFO, "3.1.1. OutputStreamImpl::checkStatus the block is in the SSD bucket");
                        vector<int32_t> newPingBlockVector;
                        newPingBlockVector.push_back(blockID);
                        filesystem->checkAndAddPingBlockID((char *) fileName.data(), newPingBlockVector);
                        return;
                    } else {
                        //3.1.2 the block is in the OSS
                        LOG(INFO, "3.1.2. OutputStreamImpl::checkStatus the block is in the OSS");
                        filesystem->catchUpFileStatusFromLog((char *) fileName.data());
                        filesystem->writeDataFromOSS2Bucket(blockIndex, fileName);
                    }
                } else {
                    //3.2.the block is in the OSS
                    filesystem->writeDataFromOSS2Bucket(blockIndex, fileName);
                    LOG(INFO, "3.2. OutputStreamImpl::checkStatus the block is in OSS");
                }
            } else {
                LOG(INFO, "2. OutputStreamImpl::checkStatus see the block is in the SSD bucket ");
            }
        }


        void OutputStreamImpl::seekToNextBlock() {
            this->cursorIndex++;
            if (cursorIndex < status->getBlockIdVector().size()) {
                this->cursorBucketID = status->getBlockIdVector()[cursorIndex];
            }
            //1. set the last bucket
            LOG(INFO, "OutputStreamImpl::seekToNextBlock before. cursorBucketID=%d, last bucket id =%d ",
                cursorBucketID, status->getLastBucket());
            status->setLastBucket(cursorBucketID);
            LOG(INFO, "OutputStreamImpl::seekToNextBlock after. cursorBucketID=%d, last bucket id = %d", cursorBucketID,
                status->getLastBucket());

            this->cursorOffset = 0;
            //2. seek the offset of the bucket file
            this->filesystem->fsSeek(cursorBucketID * SIZE_OF_BLOCK + cursorOffset, SEEK_SET);
        }

        void OutputStreamImpl::setError(const exception_ptr &error) {

        }


    }
}
