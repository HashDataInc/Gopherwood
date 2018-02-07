
#include <cstdio>
#include <cstdint>

#include "gopherwood.h"
#include "FileSystem.h"
#include "InputStream.h"
#include "OutputStream.h"
#include "Exception.h"
#include "Logger.h"
#include "XmlConfig.h"
#include "fcntl.h"
#include "Logger.h"
#include "../common/Logger.h"

#ifndef ERROR_MESSAGE_BUFFER_SIZE
#define ERROR_MESSAGE_BUFFER_SIZE 4096
#endif

static THREAD_LOCAL char ErrorMessage[ERROR_MESSAGE_BUFFER_SIZE] = "Success";

static void SetLastException(Gopherwood::exception_ptr e) {
    std::string buffer;
    const char *p;
    p = Gopherwood::Internal::GetExceptionMessage(e, buffer);
    strncpy(ErrorMessage, p, sizeof(ErrorMessage) - 1);
    ErrorMessage[sizeof(ErrorMessage) - 1] = 0;
}

static void SetErrorMessage(const char *msg) {
    assert(NULL != msg);
    strncpy(ErrorMessage, msg, sizeof(ErrorMessage) - 1);
    ErrorMessage[sizeof(ErrorMessage) - 1] = 0;
}

#define PARAMETER_ASSERT(para, retval, eno) \
    if (!(para)) { \
        SetErrorMessage(Gopherwood::Internal::GetSystemErrorInfo(eno)); \
        errno = eno; \
        return retval; \
    }

using Gopherwood::InputStream;
using Gopherwood::OutputStream;
using Gopherwood::FileSystem;
using Gopherwood::exception_ptr;
using Gopherwood::Config;
using Gopherwood::Internal::Logger;
using std::shared_ptr;


struct GWFileSystemInternalWrapper {
public:
    GWFileSystemInternalWrapper(FileSystem *fs) :
            filesystem(fs) {
    }

    ~GWFileSystemInternalWrapper() {
        delete filesystem;
    }

    FileSystem &getFilesystem() {
        return *filesystem;
    }

private:
    FileSystem *filesystem;
};


struct GWFileInternalWrapper {
public:
    GWFileInternalWrapper() :
            input(true), inputAndOutput(false), inputStream(NULL), outputStream(NULL) {
    }

    ~GWFileInternalWrapper() {
        if (input) {
            delete static_cast<InputStream *>(inputStream);
        } else if (inputAndOutput) {
            delete static_cast<OutputStream *>(outputStream);
            delete static_cast<InputStream *>(inputStream);
        } else {
            delete static_cast<OutputStream *>(outputStream);
        }
    }

    InputStream &getInputStream() {
        if (!input && !inputAndOutput) {
            THROW(Gopherwood::GopherwoodException,
                  "Internal error: file was not opened for read.");
        }

        if (!inputStream) {
            THROW(Gopherwood::GopherwoodIOException, "Inputstream. File is not opened.");
        }

        return *static_cast<InputStream *>(inputStream);
    }

    OutputStream &getOutputStream() {
        if (!output && !inputAndOutput) {
            THROW(Gopherwood::GopherwoodException,
                  "Internal error: file was not opened for write.");
        }

        if (!outputStream) {
            THROW(Gopherwood::GopherwoodIOException, "OutputStream. File is not opened.");
        }

        return *static_cast<OutputStream *>(outputStream);
    }

    bool isInput() const {
        return input;
    }

    bool isOutput() const {
        return output;
    }

    bool isInputAndOutput() const {
        return inputAndOutput;
    }

    void setInput(bool input) {
        this->input = input;
        this->output = !input;
        this->inputAndOutput = !input;
    }

    void setOutput(bool output) {
        this->output = output;
        this->input = !output;
        this->inputAndOutput = !output;
    }

    void setInputAndOutput(bool inputAndOutput) {
        this->inputAndOutput = inputAndOutput;
        this->input = !inputAndOutput;
        this->output = !inputAndOutput;
    }

    void setInputStream(void *stream) {
        this->inputStream = stream;
    }

    void setOutputStream(void *stream) {
        this->outputStream = stream;
    }

private:
    bool input;
    bool output;
    bool inputAndOutput;
    void *inputStream;
    void *outputStream;

};

class DefaultConfig {
public:
    DefaultConfig() : conf(new Gopherwood::Config) {
        bool reportError = false;
        const char *env = getenv("LIBHDFS3_CONF");
        std::string confPath = env ? env : "";

        if (!confPath.empty()) {
            size_t pos = confPath.find_first_of('=');

            if (pos != confPath.npos) {
                confPath = confPath.c_str() + pos + 1;
            }

            reportError = true;
        } else {
            confPath = "hdfs-client.xml";
        }

        init(confPath, reportError);
    }

    DefaultConfig(const char *path) : conf(new Gopherwood::Config) {
        assert(path != NULL && strlen(path) > 0);
        init(path, true);
    }

    std::shared_ptr<Config> getConfig() {
        return conf;
    }

private:
    void init(const std::string &confPath, bool reportError) {
        if (access(confPath.c_str(), R_OK)) {
            if (reportError) {
                LOG(Gopherwood::Internal::LOG_ERROR,
                    "Environment variable GOPHERWOOD_CONF is set but %s cannot be read",
                    confPath.c_str());
            } else {
                return;
            }
        }

        conf->update(confPath.c_str());
    }

private:
    std::shared_ptr<Config> conf;
};


gopherwoodFS gwCreateContext(char *fileName) {
    gopherwoodFS retVal = NULL;


    FileSystem *fs = NULL;

    fs = new FileSystem(fileName);

    retVal = new GWFileSystemInternalWrapper(fs);

    return retVal;
}

//TODO
tSize gwRead(gopherwoodFS fs, gwFile file, void *buffer, tSize length) {
    return file->getInputStream().read(static_cast<char *>(buffer), length);
}


gwFile gwOpenFile(gopherwoodFS fs, const char *fileName, int flags) {
    GWFileInternalWrapper *file = NULL;
    OutputStream *os = NULL;
    InputStream *is = NULL;
    try {
        file = new GWFileInternalWrapper();

        if ((flags & O_CREAT) || (flags & O_APPEND) || (flags & O_WRONLY)) {
            LOG(Gopherwood::Internal::INFO, "gwOpenFile the mode is write only");
            int internalFlags = Gopherwood::WriteOnly;
            file->setOutput(true);
            os = new OutputStream(fs->getFilesystem(), (char *) fileName, internalFlags);
            file->setOutputStream(os);
        } else if ((flags & O_RDONLY)) {
            LOG(Gopherwood::Internal::INFO, "gwOpenFile the mode is read only");
            file->setInput(true);
            is = new InputStream(fs->getFilesystem(), fileName, true);
            file->setInputStream(is);
        } else {
            LOG(Gopherwood::Internal::INFO, "gwOpenFile the mode is read and write");

            file->setInputAndOutput(true);
            is = new InputStream(fs->getFilesystem(), fileName, true);
            file->setInputStream(is);

            int internalFlags = Gopherwood::ReadWrite;
            os = new OutputStream(fs->getFilesystem(), (char *) fileName, internalFlags);
            file->setOutputStream(os);
        }

        return file;
    } catch (const std::bad_alloc &e) {
        delete file;
        delete os;
        delete is;
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        delete file;
        delete os;
        delete is;
        SetLastException(Gopherwood::current_exception());
    }

    return NULL;
}


int gwSeek(gopherwoodFS fs, gwFile file, tOffset desiredPos) {
    try {
        if (file->isInput()) {
            file->getInputStream().seek(desiredPos);
        } else if (file->isOutput()) {
            file->getOutputStream().seek(desiredPos);
        } else {
            file->getInputStream().seek(desiredPos);
            file->getOutputStream().seek(desiredPos);
        }
        return 0;
    } catch (const std::bad_alloc &e) {
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Gopherwood::current_exception());
    }

    return -1;

}


int32_t gwWrite(gopherwoodFS fs, gwFile file, const void *buffer, tSize length) {
//    PARAMETER_ASSERT(fs && file && buffer && length > 0, -1, EINVAL);
//    PARAMETER_ASSERT(!file->isInput(), -1, EINVAL);

    try {
        file->getOutputStream().write(static_cast<const char *>(buffer), length);
        return length;
    } catch (const std::bad_alloc &e) {
        LOG(Gopherwood::Internal::LOG_ERROR, "Out of memory");
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        SetLastException(Gopherwood::current_exception());
    }

    return -1;
}


int gwCloseFile(gopherwoodFS fs, gwFile file) {
    try {
        if (file) {
            if (file->isInput()) {
                LOG(Gopherwood::Internal::INFO, "gwCloseFile. this is the inputStream");
                file->getInputStream().close();
            } else if (file->isOutput()) {
                LOG(Gopherwood::Internal::INFO, "gwCloseFile. this is the outputStream");
                file->getOutputStream().close();
            } else {
                LOG(Gopherwood::Internal::INFO, "gwCloseFile. this is the input and outputStream");
                //BUG-FIX. just one close is enough, because they share the same FileSystem object
                file->getOutputStream().close();
            }
            delete file;
        }

        return 0;
    } catch (const std::bad_alloc &e) {
        delete file;
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        delete file;
        SetLastException(Gopherwood::current_exception());
    }

    return -1;
}


int deleteFile(gopherwoodFS fs, gwFile file) {
    try {
        if (file) {
            if (file->isInput()) {
                LOG(Gopherwood::Internal::INFO, "deleteFile. this is the inputStream");
                file->getInputStream().deleteFile();

            } else if (file->isOutput()) {
                LOG(Gopherwood::Internal::INFO, "deleteFile. this is the outputStream");
                file->getOutputStream().deleteFile();

            } else {
                LOG(Gopherwood::Internal::INFO, "deleteFile. this is the input and outputStream");
                //TODO, need think more
                //BUG-FIX. just one close is enough, because they share the same FileSystem object
                file->getInputStream().deleteFile();


            }
            delete file;
        }

        return 0;
    } catch (const std::bad_alloc &e) {
        delete file;
        SetErrorMessage("Out of memory");
        errno = ENOMEM;
    } catch (...) {
        delete file;
        SetLastException(Gopherwood::current_exception());
    }

    return -1;
}









