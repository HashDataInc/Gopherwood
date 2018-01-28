//
// Created by root on 11/19/17.
//


#include "InputStream.h"


using namespace Gopherwood::Internal;

namespace Gopherwood {


    InputStream::InputStream(FileSystem &fs, const char *fileName, bool verifyChecksum) {

        if (!fs.impl) {
            THROW(GopherwoodIOException, "FileSystem: not connected.");
        }

        impl = new Internal::InputStreamImpl(fs.impl->filesystem, fileName, verifyChecksum);

    }

    InputStream::~InputStream() {
        delete impl;
    }

    int32_t InputStream::read(char *buf, int32_t size) {
        return impl->read(buf, size);
    }

//    void InputStream::open(FileSystem &fs, const char *fileName, bool verifyChecksum = true){
//        if (!fs.impl) {
//            THROW(GopherwoodException, "FileSystem: not connected.");
//        }
//
//        impl->open(fs.impl->filesystem, fileName, verifyChecksum);
//
//    }

    void InputStream::seek(int64_t pos){
        impl->seek(pos);
    }

    void InputStream::close() {
        impl->close();
    }
}



