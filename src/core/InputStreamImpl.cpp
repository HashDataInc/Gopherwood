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
            LOG(INFO, "InputStreamImpl::InputStreamImpl come in here");
            //1.check the file exist in the fileStatusMap or not
            bool exist = filesystem->checkFileExist((char *) fileName);
            LOG(INFO, "InputStreamImpl::InputStreamImpl come in here exist=%d", exist);
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
//            seek(0);
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
                int64_t pos = cursorIndex * SIZE_OF_BLOCK + cursorOffset;
                if (pos >= filesystem->getTheEOFOffset(fileName.c_str())) {
                    *buf = NULL;
                    LOG(INFO,
                        "InputStreamImpl::read. the seek pos exceed the file size which pos =%d, and the endOffsetOfBucket=%d ",
                        pos, filesystem->getTheEOFOffset(fileName.c_str()));
                    return 0;
                }
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

                //1.get the lock 2. seek to the offset 3. read the data 4. release the lock
                filesystem->getLock();
                this->filesystem->fsSeek(cursorBucketID * SIZE_OF_BLOCK + cursorOffset, SEEK_SET);
                int64_t readLength = filesystem->readDataFromBucket(buf, bufLength);
                filesystem->releaseLock();


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

                    //1.get the lock 2. seek to the offset 3. read the data 4. release the lock
                    filesystem->getLock();
                    this->filesystem->fsSeek(cursorBucketID * SIZE_OF_BLOCK + cursorOffset, SEEK_SET);
                    int64_t readLength = filesystem->readDataFromBucket(buf, size);
                    filesystem->releaseLock();

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

                    // 1.get the lock 2. seek to the offset 3. read the data 4. release the lock
                    filesystem->getLock();
                    this->filesystem->fsSeek(cursorBucketID * SIZE_OF_BLOCK + cursorOffset, SEEK_SET);
                    int64_t readLength = filesystem->readDataFromBucket(buf, remainOffsetInBlock);
                    filesystem->releaseLock();

                    int totalOffset = 0;
                    totalOffset += readLength;
                    bufLength -= readLength;

                    while (bufLength > 0) {
                        if (bufLength > SIZE_OF_BLOCK) {

                            filesystem->getLock();
                            seekToNextBlock();
                            readLength = filesystem->readDataFromBucket(buf + totalOffset, SIZE_OF_BLOCK);
                            filesystem->releaseLock();


                            bufLength -= readLength;
                            totalOffset += readLength;
                            cursorOffset += readLength;
                        } else {

                            filesystem->getLock();
                            seekToNextBlock();
                            readLength = filesystem->readDataFromBucket(buf + totalOffset, bufLength);
                            filesystem->releaseLock();

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


        int64_t InputStreamImpl::seek(int64_t pos) {
            int64_t retOffset = checkStatus(pos);
            try {
                if (retOffset > 0) {
                    seekInternal(pos);
                }
            } catch (...) {
                throw;
            }

            return retOffset;
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

        int64_t InputStreamImpl::checkStatus(int64_t pos) {
            LOG(INFO, "InputStreamImpl::checkStatus pos = %d", pos);
            if (pos < 0) {
                LOG(LOG_ERROR, "InputStreamImpl::checkStatus pos can not be smaller than zero");
                return -1;
            }
            //1. check the size of the file
            int64_t theEOFOffset = this->filesystem->getTheEOFOffset(this->fileName.data());
            LOG(INFO, "InputStreamImpl::checkStatus. theEOFOffset=%d", theEOFOffset);

            if (theEOFOffset == 0) {
                LOG(INFO, "InputStreamImpl::checkStatus. the file do not contain any one bucket");
                return 0;
            }
            if (status->getBlockIdVector().size() == 0) {
                LOG(LOG_ERROR, "InputStreamImpl::checkStatus. do not contain any file");
                return -1;
            }

            if (pos > theEOFOffset) {
                //todo, throw error
                LOG(LOG_ERROR, "error, the given pos exceed the size of the file");
                return -1;
            }

            //2. get the blockID which seeks to
            int64_t blockIndex = pos / SIZE_OF_BLOCK;
            int blockID = status->getBlockIdVector()[blockIndex];

            LOG(INFO, "InputStreamImpl::checkStatus. blockID=%d", blockID);

            //3. check the blockID is PING or not.(its type='1' or not)
            if (status->getLruCache()->get(blockID)) {
                return pos;
            }


            /*********************************DOTO FOR TEST********************/
            LOG(INFO, "InputStreamImpl::checkStatus. start of the status print lru cache");
            status->getLruCache()->printLruCache();
            LOG(INFO, "InputStreamImpl::checkStatus. end of the status print lru cache");
            /*********************************DOTO FOR TEST********************/

            //3.1 see the block is in the SSD bucket(not ping) or in the OSS.
            if (blockID >= 0) {
                //3.1.1 the block is in the SSD bucket
                bool isEqual = filesystem->checkBlockIDWithFileName(blockID, fileName);
                LOG(INFO, "InputStreamImpl::checkStatus isEqual=%d", isEqual);
                if (isEqual) {
                    LOG(INFO, "3.1.1. InputStreamImpl::checkStatus the block is in the SSD bucket");
                    vector<int32_t> newPingBlockVector;
                    newPingBlockVector.push_back(blockID);
                    filesystem->checkAndAddPingBlockID((char *) fileName.data(), newPingBlockVector);
                    return pos;
                } else {
                    //3.1.2 the block is in the OSS. this means the block have been evicted to the OSS
                    LOG(INFO, "3.1.2. InputStreamImpl::checkStatus the block is in the OSS");
                    filesystem->catchUpFileStatusFromLog((char *) fileName.data());
                    filesystem->writeDataFromOSS2Bucket(blockIndex, fileName);
                }
            } else {
                //3.2.the block is in the OSS
                LOG(INFO, "3.2. InputStreamImpl::checkStatus the block is in OSS");
                filesystem->writeDataFromOSS2Bucket(blockIndex, fileName);
            }
            return pos;
        }


        std::shared_ptr<FileStatus> InputStreamImpl::getFileStatus() {
            this->status = filesystem->getFileStatus(fileName.data());
            LOG(INFO, "InputStreamImpl::getFileStatus, status->getFileSize()=%d", status->getFileSize());
            return status;
        }

        void InputStreamImpl::deleteFile() {
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


        void InputStreamImpl::deleteFileBucket(int64_t pos) {
            LOG(INFO, "InputStreamImpl::deleteFileBucket pos = %d", pos);
            if (pos < 0) {
                LOG(LOG_ERROR, "InputStreamImpl::deleteFileBucket pos can not be smaller than zero");
            }
            //1. check the size of the file
            int64_t theEOFOffset = this->filesystem->getTheEOFOffset(this->fileName.data());
            LOG(INFO, "InputStreamImpl::deleteFileBucket. theEOFOffset=%d", theEOFOffset);

            if (theEOFOffset == 0) {
                LOG(INFO, "the file do not contain any one bucket");
                return;
            }
            if (status->getBlockIdVector().size() == 0) {
                LOG(LOG_ERROR, "InputStreamImpl::deleteFileBucket. do not contain any file");
                return;
            }

            if (pos > theEOFOffset) {
                //todo, throw error
                LOG(LOG_ERROR, "InputStreamImpl::deleteFileBucket. error, the given pos exceed the size of the file");
                return;
            }

            //2. get the blockID which seeks to
            int64_t blockIndex = pos / SIZE_OF_BLOCK;
            int blockID = status->getBlockIdVector()[blockIndex];

            LOG(INFO, "InputStreamImpl::deleteFileBucket. blockID=%d", blockID);

            //3. check the blockID is PING or not.(its type='1' or not)
            if (status->getLruCache()->get(blockID)) {
                std::vector<int32_t> deleteBlockVector;
                deleteBlockVector.push_back(blockID);
                filesystem->deleteBlockFromSSD((char *) fileName.c_str(), deleteBlockVector);
                return;
            }


            /*********************************DOTO FOR TEST********************/
            LOG(INFO, "InputStreamImpl::deleteFileBucket. start of the status print lru cache");
            status->getLruCache()->printLruCache();
            LOG(INFO, "InputStreamImpl::deleteFileBucket. end of the status print lru cache");
            /*********************************DOTO FOR TEST********************/

            //3.1 see the block is in the SSD bucket(not ping) or in the OSS.
            if (blockID >= 0) {
                //3.1.1 the block is in the SSD bucket
                bool isEqual = filesystem->checkBlockIDWithFileName(blockID, fileName);
                LOG(INFO, "InputStreamImpl::deleteFileBucket isEqual=%d", isEqual);
                if (isEqual) {
                    LOG(INFO, "3.1.1. InputStreamImpl::deleteFileBucket the block is in the SSD bucket");
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
                LOG(INFO, "3.2. InputStreamImpl::deleteFileBucket the block is in OSS");
            }
        }

    }
}