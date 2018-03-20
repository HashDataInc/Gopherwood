#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "client/gopherwood.h"

static gopherwoodFS fs;
char workDir[] = "/tmp/gopherwood";


void testReadWrite(){
    char input[] = "aaaaaaaaaabbbbbbbbbbcccccccccc";
    char* buffer = (char*)malloc(100);

    gwFile file = gwOpenFile(fs, "/test1", GW_CREAT|GW_RDWR);
    gwWrite(fs, file, input, sizeof(input));
    gwFlush(fs, file);

    gwSeek(fs, file, 10, SEEK_SET);
    int len = gwRead(fs, file, buffer, 20);
    buffer[len] = '\0';
    printf("Read From Gopherwood the first time %s \n", buffer);
    buffer[0] = '\0';

    gwFile file1 = gwOpenFile(fs, "/test1", GW_RDONLY);

    gwSeek(fs, file1, 10, SEEK_SET);
    len = gwRead(fs, file1, buffer, 20);
    buffer[len] = '\0';
    printf("Read From Gopherwood the second time %s \n", buffer);

    gwCloseFile(fs, file1);
    gwCloseFile(fs, file);

    gwDeleteFile(fs, "/test1");
};

int main(int argc, char *argv[])
{
    gwFormatContext(workDir);

    GWContextConfig config;
    config.blockSize = 10;
    config.numBlocks = 100;

    fs =  gwCreateContext(workDir, &config);

    testReadWrite();

    gwDestroyContext(fs);

    return 0;
}