//
// Created by root on 11/18/17.
//


#include "InputStreamImpl.h"
#include "../common/Logger.h"

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
            //2. the fileName exist in the fileStatusMap, so catch up the fileStatus from LOG.
            //TODO ,we should mark the offset that last time catch.
            filesystem->catchUpFileStatusFromLog(0);

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
            checkStatus();
            try {
                int32_t done = readInternal(buf, size);
                return done;
            } catch (...) {
                throw;
            }
        }


        int32_t InputStreamImpl::readInternal(char *buf, int32_t size) {

            int64_t remainOffsetInBlock = SIZE_OF_BLOCK - cursorOffset;
            if (size <= remainOffsetInBlock) {
                filesystem->readDataFromBucket(buf, size);
                cursorOffset += size;
            } else {
                int64_t remainOffsetTotal = getRemainLength();

                int64_t bufLength = size < remainOffsetTotal ? size : remainOffsetTotal;

                char resBuf[bufLength];

                char tmpBuf[SIZE_OF_BLOCK];

                int64_t sizeRemain = bufLength;
                int64_t bufOffset = 0;

                //read the first block data of the file.
                filesystem->readDataFromBucket(tmpBuf, remainOffsetInBlock);
                sizeRemain -= remainOffsetInBlock;
                memcpy(resBuf + bufOffset, tmpBuf, remainOffsetInBlock);
                bufOffset += remainOffsetInBlock;

                while (sizeRemain > 0) {
                    if (sizeRemain > SIZE_OF_BLOCK) {
                        seekToNextBlock();
                        filesystem->readDataFromBucket(tmpBuf, SIZE_OF_BLOCK);
                        sizeRemain -= SIZE_OF_BLOCK;
                        memcpy(resBuf + bufOffset, tmpBuf, SIZE_OF_BLOCK);
                        bufOffset += SIZE_OF_BLOCK;

                        cursorOffset = SIZE_OF_BLOCK;
                    } else {
                        seekToNextBlock();
                        filesystem->readDataFromBucket(tmpBuf, sizeRemain);
                        sizeRemain -= sizeRemain;
                        memcpy(resBuf + bufOffset, tmpBuf, sizeRemain);
                        bufOffset += sizeRemain;

                        cursorOffset = sizeRemain;
                    }
                }
                memcpy(buf, resBuf, sizeof(resBuf));
            }

        }


        void InputStreamImpl::seekToNextBlock() {
            this->cursorIndex++;
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
            checkStatus();
            try {
                seekInternal(pos);
            } catch (...) {
                throw;
            }

        }

        void InputStreamImpl::seekInternal(int64_t pos) {
            int64_t theEOFOffset = this->filesystem->getTheEOFOffset(this->fileName.data());
            if(theEOFOffset==0){
                LOG(INFO, "the file do not contain any one bucket");
                return ;
            }

            if (theEOFOffset > pos) {
                //todo, throw error
                LOG(LOG_ERROR, "error, the given pos exceed the size of the file");
            }
            int32_t bucketIDIndex = pos / SIZE_OF_BLOCK;
            int64_t bucketOffset = pos % SIZE_OF_BLOCK;
            //TODO, should this in the FileSystem or in the InputStream?
            this->status = filesystem->getFileStatus(fileName.data());
            this->cursorIndex = bucketIDIndex;
            LOG(INFO, "InputStreamImpl cursorIndex = %d",cursorIndex);
            this->cursorBucketID = status->getBlockIdVector()[cursorIndex];
            LOG(INFO, "InputStreamImpl cursorBucketID = %d",cursorBucketID);
            this->cursorOffset = bucketOffset;

            //seek the offset of the bucket file
            this->filesystem->fsSeek(cursorBucketID * SIZE_OF_BLOCK + cursorOffset, SEEK_SET);
        }


        int64_t InputStreamImpl::tell() {

        }


        //TODO,
        void InputStreamImpl::close() {

            filesystem->closeFile((char*)fileName.data());


        }


        string InputStreamImpl::toString() {

        }

        void InputStreamImpl::checkStatus() {

        }


    }
}