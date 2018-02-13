#ifndef _GOPHERWOOD_CORE_FILESYSTEM_H_
#define _GOPHERWOOD_CORE_FILESYSTEM_H_

#include <vector>
#include <cstdint>
#include <unordered_map>
#include <cstdio>

#include "FileStatus.h"
#include "FileSystemInter.h"
#include "FileSystemImpl.h"

namespace Gopherwood {
namespace Internal {
struct FileSystemWrapper;
}

class FileSystem {
public:

    FileSystem(char *fileName);

    /**
     * Construct a FileSystem
     * @param conf gopherwood configuration
     */
    FileSystem();

    /**
     * Assign operator of FileSystem
     */
    FileSystem &operator=(const FileSystem &other);

    /**
     * Destroy a HdfsFileSystem instance
     */
    ~FileSystem();

    /**
     * check whether the ssd file exist or not
     * @return 1 if exist, -1 otherwise
     */
    int32_t checkSSDFile();

    /**
     * check whether the shared memory exist or not
     * @return 1 if exist, -1 otherwise
     */
    int32_t checkSharedMemory();

    /**
     * delete the shared memory
     * @return
     */
    int32_t deleteSharedMemory();

    void createFile(char *fileName);

    void acquireBlock();

//TODO , THIS IS private, for test convenient, set it to public
public:
//        Config conf;
    Internal::FileSystemWrapper *impl;

    friend class InputStream;

    friend class OutputStream;

private:

};

}

#endif //_GOPHERWOOD_CORE_FILESYSTEM_H_
