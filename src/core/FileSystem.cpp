//
// Created by root on 11/19/17.
//

#include "FileSystem.h"

using namespace Gopherwood::Internal;

namespace Gopherwood {

namespace Internal {

}

static FileSystemWrapper *createContextInternal(char *fileName) {
    return new FileSystemWrapper(std::shared_ptr < FileSystemInter > (new FileSystemImpl(fileName)));
}

FileSystem::~FileSystem() {

}

FileSystem::FileSystem(char *fileName) {
    impl = createContextInternal(fileName);
}

FileSystem::FileSystem() {

//        this->conf = conf;

}

}
