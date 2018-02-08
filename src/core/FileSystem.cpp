//
// Created by root on 11/19/17.
//

#include "FileSystem.h"
#include "../common/ExceptionInternal.h"

using namespace Gopherwood::Internal;

namespace Gopherwood {

    namespace Internal {

    }

    static FileSystemWrapper *createContextInternal() {
        return new FileSystemWrapper(std::shared_ptr<FileSystemInter>(new FileSystemImpl()));
    }

    FileSystem::~FileSystem() {


    }

    FileSystem::FileSystem() {
        impl = createContextInternal();
    }


    FileStatus FileSystem::getFileStatus(char *fileName) {
        if (!impl) {
            THROW(GopherwoodException, "FileSystem: not connected.");
        }

        std::shared_ptr<FileStatus> status = impl->filesystem->getFileStatus(fileName);
    }

    int FileSystem::destroyFileSystem() {
        int res = impl->filesystem->destroyFileSystem();
        return res;
    }


}