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

    void OutputStream::seek(int64_t pos) {
        impl->seek(pos);
    }

    void OutputStream::write(const char *buf, int64_t size) {
        impl->write(buf,size);
    }

    void OutputStream::close(){
        impl->close();
    }

}