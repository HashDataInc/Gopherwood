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


    private:
//        Config conf;
        Internal::FileSystemWrapper *impl;

        friend class InputStream;

        friend class OutputStream;

    private:
        /**
       *  create the shared memory
       * @return the key of the shared memory
//       */
//        int32_t createSharedMemory();
//
//
//        /**
//         * rebuild the FileStatus from the log file
//         * @param fileName  the file name
//         * @return the file status of the file
//         */
//        unordered_map<string, std::shared_ptr<FileStatus>> rebuildFileStatusFromLog(char *fileName);
//
//        unordered_map<string, std::shared_ptr<FileStatus>> chaseFileStatusFromLog(int64_t logOffset);

    };

}

#endif //_GOPHERWOOD_CORE_FILESYSTEM_H_