//
// Created by root on 11/18/17.
//

#ifndef _GOPHERWOOD_CORE_OUTPUTSTREAMIMPL_H_
#define _GOPHERWOOD_CORE_OUTPUTSTREAMIMPL_H_

#include <iostream>
#include <memory>
#include "OutputStreamInter.h"
#include "Exception.h"
#include "OutputStream.h"
#include "Logger.h"

namespace Gopherwood {
    namespace Internal {
        class OutputStreamImpl : public OutputStreamInter {

        public:

            OutputStreamImpl(std::shared_ptr<FileSystemInter> fs, char *fileName, int flag);

            ~OutputStreamImpl();


            /**
           * To create or append a file.
           * @param fs gopherwood file system.
           * @param fileName the file name.
           * @param flag creation flag, can be Create, Append or Create|Overwrite.
           */
//            void open(std::shared_ptr<FileSystemInter> fs, char *fileName, int flag);

            /**
             * To write data to file.
             * @param buf the data used to write.
             * @param size the data size.
             */
            void write(const char *buf, int64_t size);

            /**
             * Flush all data in buffer and waiting for ack.
             * Will block until get all acks.
             */
            void flush();

            /**
             * return the current file length.
             * @return current file length.
             */
            int64_t tell();

            /**
             * the same as flush right now.
             */
            void sync();

            /**
             * close the stream.
             */
            void close();


            string toString();

            void setError(const exception_ptr &error);

            int64_t seek(int64_t pos);

            void deleteFileBucket(int64_t pos);

            void deleteFile();

            std::shared_ptr<FileStatus> getFileStatus();

        private:
            std::shared_ptr<FileSystemInter> filesystem;
            int32_t cursorBucketID = 0; // the cursor bucket id of the output stream
            int32_t cursorIndex = 0;//the index of the cursorBucketID. status->getBlockIdVector()[cursorIndex] = cursorBucketID
            int64_t cursorOffset = 0;// the cursor offset of the output stream
            string fileName;
            std::shared_ptr<FileStatus> status;


        private:
//            void createFile(char *fileName);

            int64_t checkStatus(int64_t pos);

            void seekInternal(int64_t pos);

            void writeInternal(char *buf, int64_t size);

            int64_t getRemainLength();

            void seekToNextBlock();
        };


    }
}


#endif //_GOPHERWOOD_CORE_OUTPUTSTREAMIMPL_H_
