//
// Created by root on 11/19/17.
//


#include "OutputStream.h"

using namespace Gopherwood::Internal;

namespace Gopherwood {


    OutputStream::OutputStream(FileSystem &fs, char *fileName, int flag) {


        if (!fs.impl) {
            THROW(GopherwoodIOException, "FileSystem: not connected.");
        }

        this->impl = new Internal::OutputStreamImpl(fs.impl->filesystem, fileName, flag);
    }

    OutputStream::~OutputStream() {
        delete impl;
    }

    int64_t OutputStream::seek(int64_t pos) {
        return impl->seek(pos);
    }

    void OutputStream::write(const char *buf, int64_t size) {
        impl->write(buf,size);
    }

    void OutputStream::close(){
        impl->close();
    }

    void OutputStream::deleteFile(){
        impl->deleteFile();
    }

    std::shared_ptr<FileStatus> OutputStream::getFileStatus() {
        std::shared_ptr<FileStatus> status = impl->getFileStatus();
        LOG(INFO, "OutputStream::getFileStatus. status->getFileSize()=%d", status->getFileSize());
        return status;
    }

}