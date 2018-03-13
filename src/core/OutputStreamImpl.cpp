
#include "OutputStreamImpl.h"
#include "../common/Logger.h"

namespace Gopherwood {
    namespace Internal {
        OutputStreamImpl::~OutputStreamImpl() {

        }

        OutputStreamImpl::OutputStreamImpl(std::shared_ptr<FileSystemInter> fs, char *fileName, int flag) {

            this->filesystem = fs;
            this->fileName = fileName;

            //1.check the file exist in the fileStatusMap or not
            bool exist = filesystem->checkFileExist(fileName);
            LOG(INFO, "OutputStreamImpl::OutputStreamImpl. exist =  %d", exist);

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
            LOG(INFO,
                "OutputStreamImpl::OutputStreamImpl. getBlockIdVector().size() = %d,status->getLastBucket() = %d, status->getEndOffsetOfBucket()=%d",
                status->getBlockIdVector().size(), status->getLastBucket(), status->getEndOffsetOfBucket());
            int64_t endOfOffset = filesystem->getTheEOFOffset(fileName);
            LOG(INFO, "OutputStreamImpl::OutputStreamImpl. endOfOffset = %d", endOfOffset);

            if (status->getBlockIdVector().size() == 0) {
                LOG(INFO,
                    "OutputStreamImpl::OutputStreamImpl. openFile do not seek, because the file do not contain any bucket");
                //do nothing, because the file do not contain any bucket.
            } else {
                seek(endOfOffset);
            }

        }


        std::shared_ptr<FileStatus> OutputStreamImpl::getFileStatus() {
            this->status = filesystem->getFileStatus(fileName.data());
            LOG(INFO, "OutputStreamImpl::getFileStatus, status->getFileSize()=%d", status->getFileSize());
            return status;
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
            LOG(INFO, "OutputStreamImpl::writeInternal. cursorOffset = %d, cursorIndex=%d,cursorBucketID=%d",
                cursorOffset, cursorIndex, cursorBucketID);
            LOG(INFO, "OutputStreamImpl::writeInternal. data write size = %d, remainOffsetInBlock=%d", size,
                remainOffsetInBlock);
            if (size < remainOffsetInBlock) {

                filesystem->getLock();
                filesystem->fsSeek(cursorBucketID * SIZE_OF_BLOCK + cursorOffset, SEEK_SET);
                filesystem->writeDataToBucket(buf, size);
                filesystem->releaseLock();

                cursorOffset += size;

                //3. set the endOffsetOfBucket
                status->setEndOffsetOfBucket(cursorOffset);
                LOG(INFO, "OutputStreamImpl::writeInternal.1.  cursorOffset=%d", cursorOffset);
            } else {
                int64_t remainOffsetTotal = getRemainLength();

                //TODO, this maybe wrong, because, I don't know when acquire a block, it will sync with the filesystem/FileStatus or not.
                //TODO, if it does not work, we should check the code and try another method.
                while ((size >= remainOffsetTotal)) {
                    LOG(INFO, "OutputStreamImpl::writeInternal. 2. remainOffsetTotal before =%d", remainOffsetTotal);
                    //1&2 acquire one block, sync to the shared memory and LOG system.
                    filesystem->acquireNewBlock((char *) fileName.data());
                    int64_t afterRemainOffsetTotal = getRemainLength();

                    remainOffsetTotal = afterRemainOffsetTotal;
                    LOG(INFO, "OutputStreamImpl::writeInternal. 2. remainOffsetTotal after =%d", remainOffsetTotal);
                }

                //1. BUG-FIX, IMPORTANT. because, when above code execute, especially the acquireNewBlock method execute,

                filesystem->getLock();
                // it will change the ssd bucket's offset.
                filesystem->fsSeek(cursorBucketID * SIZE_OF_BLOCK + cursorOffset, SEEK_SET);
                //2. write remainOffsetInBlock data to the bucket first;
                filesystem->writeDataToBucket(buf, remainOffsetInBlock);
                filesystem->releaseLock();

                int64_t haveWriteSize = remainOffsetInBlock;
                int64_t remainSize = size - haveWriteSize;

                LOG(INFO, "OutputStreamImpl::writeInternal.  haveWriteSize =%d,remainSize = %d", haveWriteSize,
                    remainSize);
                if (remainSize == 0) {
                    seekToNextBlock();
                }
                while (remainSize > 0) {
                    if (remainSize > SIZE_OF_BLOCK) {

                        filesystem->getLock();
                        seekToNextBlock();
                        filesystem->writeDataToBucket(buf + haveWriteSize, SIZE_OF_BLOCK);
                        filesystem->releaseLock();

                        cursorOffset = SIZE_OF_BLOCK;
                        remainSize -= SIZE_OF_BLOCK;
                        haveWriteSize += SIZE_OF_BLOCK;
                    } else {
                        filesystem->getLock();
                        seekToNextBlock();
                        filesystem->writeDataToBucket(buf + haveWriteSize, remainSize);
                        filesystem->releaseLock();

                        cursorOffset = remainSize;
                        remainSize -= remainSize;
                        haveWriteSize += remainSize;
                        if (remainSize == 0) {
                            seekToNextBlock();
                        } else {
                            //3. set the endOffsetOfBucket
                            status->setEndOffsetOfBucket(cursorOffset);
                            LOG(INFO, "OutputStreamImpl::writeInternal.2.  cursorOffset=%d", cursorOffset);
                        }
                    }
                }
            }



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

        int64_t OutputStreamImpl::seek(int64_t pos) {
            int64_t retOffset = checkStatus(pos);
            try {
                int blockIDSize = status->getBlockIdVector().size();

                if ((blockIDSize > 0 && retOffset >= 0) || (retOffset > 0)) {
                    seekInternal(pos);
                }
            } catch (...) {
                throw;
            }
            return retOffset;
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

        int64_t OutputStreamImpl::checkStatus(int64_t pos) {
            while (status->getBlockIdVector().size() == 0) {
                LOG(INFO,
                    "OutputStreamImpl::checkStatus, the file do not contain any bucket, so create new one for write");
                filesystem->acquireNewBlock((char *) fileName.data());
            }


            LOG(INFO, "OutputStreamImpl::checkStatus pos = %d", pos);
            if (pos < 0) {
                LOG(LOG_ERROR, "OutputStreamImpl::checkStatus pos can not be smaller than zero");
                return -1;
            }
            //1. check the size of the file
            int64_t theEOFOffset = this->filesystem->getTheEOFOffset(this->fileName.data());
            LOG(INFO, "OutputStreamImpl::checkStatus. theEOFOffset=%d", theEOFOffset);

//            if (theEOFOffset == 0) {
//                LOG(INFO, "the file do not contain any one bucket");
//                return -1;
//            }
            if (status->getBlockIdVector().size() == 0) {
                LOG(LOG_ERROR, "OutputStreamImpl::checkStatus. do not contain any file");
                return -1;
            }

            if (pos > theEOFOffset) {
                //todo, throw error
                LOG(LOG_ERROR, "error, the given pos exceed the size of the file");
                return -1;
            }

            //2. get the blockID which seeks to
            int64_t blockIndex = pos / SIZE_OF_BLOCK;
            /*BUG-FIX. if the file size is the multiple of the SIZE_OF_BLOCK. for example, if the SIZE_OF_BLOCK=1024, and the pos=1024.
            so this need to acquire more blocks.*/
            while (blockIndex == status->getBlockIdVector().size()) {
                filesystem->acquireNewBlock((char *) fileName.data());
            }
            int blockID = status->getBlockIdVector()[blockIndex];

            LOG(INFO, "OutputStreamImpl::checkStatus. blockID=%d", blockID);

            //3. check the blockID is PING or not.(its type='1' or not)
            if (status->getLruCache()->get(blockID)) {
                return pos;
            }

            /*********************************todo FOR TEST********************/
            LOG(INFO, "OutputStreamImpl::checkStatus. start of the status print lru cache");
            status->getLruCache()->printLruCache();
            LOG(INFO, "OutputStreamImpl::checkStatus. end of the status print lru cache");
            /*********************************DOTO FOR TEST********************/

            //3.1 see the block is in the SSD bucket or in the OSS.
            if (blockID >= 0) {
                //3.1.1 the block is in the SSD bucket
                bool isEqual = filesystem->checkBlockIDWithFileName(blockID, fileName);

                LOG(INFO, "OutputStreamImpl::checkStatus isEqual=%d", isEqual);
                if (isEqual) {
                    LOG(INFO, "3.1.1. OutputStreamImpl::checkStatus the block is in the SSD bucket");
                    vector<int32_t> newPingBlockVector;
                    newPingBlockVector.push_back(blockID);
                    filesystem->checkAndAddPingBlockID((char *) fileName.data(), newPingBlockVector);
                    return pos;
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

            return pos;
        }


        void OutputStreamImpl::seekToNextBlock() {

            LOG(INFO, "OutputStreamImpl::seekToNextBlock before. cursorBucketID=%d, last bucket id =%d ",
                cursorBucketID, status->getLastBucket());

            this->cursorIndex++;
            if (cursorIndex < status->getBlockIdVector().size()) {
                this->cursorBucketID = status->getBlockIdVector()[cursorIndex];
            }
            this->cursorOffset = 0;

            //1. set the last bucket
            status->setLastBucket(cursorBucketID);
            status->setLastBucketIndex(cursorIndex);
            status->setEndOffsetOfBucket(cursorOffset);
            LOG(INFO, "OutputStreamImpl::seekToNextBlock after. cursorBucketID=%d, last bucket id = %d", cursorBucketID,
                status->getLastBucket());


            //2. seek the offset of the bucket file
            this->filesystem->fsSeek(cursorBucketID * SIZE_OF_BLOCK + cursorOffset, SEEK_SET);
        }

        void OutputStreamImpl::setError(const exception_ptr &error) {

        }


        void OutputStreamImpl::deleteFile() {
            //1. first catch up the file status
            filesystem->catchUpFileStatusFromLog((char *) fileName.c_str());

            //2. get the end of the file
            int64_t theEOFOffset = this->filesystem->getTheEOFOffset(this->fileName.data());

            int64_t offset = 0;

            //3. delete the file block
            while (offset < theEOFOffset) {
                deleteFileBucket(offset);
                offset += SIZE_OF_BLOCK;
            }

            //4. delete the file status
            filesystem->deleteFile((char *) fileName.c_str());
        }


        void OutputStreamImpl::deleteFileBucket(int64_t pos) {
            LOG(INFO, "OutputStreamImpl::deleteFileBucket pos = %d", pos);
            if (pos < 0) {
                LOG(LOG_ERROR, "OutputStreamImpl::deleteFileBucket pos can not be smaller than zero");
            }
            //1. check the size of the file
            int64_t theEOFOffset = this->filesystem->getTheEOFOffset(this->fileName.data());
            LOG(INFO, "OutputStreamImpl::deleteFileBucket. theEOFOffset=%d", theEOFOffset);

            if (theEOFOffset == 0) {
                LOG(INFO, "the file do not contain any one bucket");
                return;
            }
            if (status->getBlockIdVector().size() == 0) {
                LOG(LOG_ERROR, "OutputStreamImpl::deleteFileBucket. do not contain any file");
                return;
            }

            if (pos > theEOFOffset) {
                //todo, throw error
                LOG(LOG_ERROR, "OutputStreamImpl::deleteFileBucket. error, the given pos exceed the size of the file");
                return;
            }

            //2. get the blockID which seeks to
            int64_t blockIndex = pos / SIZE_OF_BLOCK;
            int blockID = status->getBlockIdVector()[blockIndex];

            LOG(INFO, "OutputStreamImpl::deleteFileBucket. blockID=%d", blockID);

            //3. check the blockID is PING or not.(its type='1' or not)
            if (status->getLruCache()->get(blockID)) {
                std::vector<int32_t> deleteBlockVector;
                deleteBlockVector.push_back(blockID);
                filesystem->deleteBlockFromSSD((char *) fileName.c_str(), deleteBlockVector);
                return;
            }


            /*********************************DOTO FOR TEST********************/
            LOG(INFO, "OutputStreamImpl::deleteFileBucket. start of the status print lru cache");
            status->getLruCache()->printLruCache();
            LOG(INFO, "OutputStreamImpl::deleteFileBucket. end of the status print lru cache");
            /*********************************DOTO FOR TEST********************/

            //3.1 see the block is in the SSD bucket(not ping) or in the OSS.
            if (blockID >= 0) {
                //3.1.1 the block is in the SSD bucket
                bool isEqual = filesystem->checkBlockIDWithFileName(blockID, fileName);
                LOG(INFO, "OutputStreamImpl::deleteFileBucket isEqual=%d", isEqual);
                if (isEqual) {
                    LOG(INFO, "3.1.1. OutputStreamImpl::deleteFileBucket the block is in the SSD bucket");
                    vector<int32_t> deleteBlockVector;
                    deleteBlockVector.push_back(blockID);
                    filesystem->deleteBlockFromSSD((char *) fileName.c_str(), deleteBlockVector);
                    return;
                } else {
                    //3.1.2 the block is in the OSS
                    filesystem->deleteBlockFromOSS(blockIndex, fileName);
                }
            } else {
                //3.2.the block is in the OSS
                filesystem->deleteBlockFromOSS(blockIndex, fileName);
                LOG(INFO, "3.2. OutputStreamImpl::deleteFileBucket the block is in OSS");
            }
        }


    }
}
