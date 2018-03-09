#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "../../src/client/gopherwood.h"

static gopherwoodFS fs;
char workDir[] = "/tmp/gopherwood";
char input[] = "aaaaaaaaaabbbbbbbbbbcccccccccc";

int main(int argc, char *argv[])
{
    char* buffer = (char*)malloc(100);

    gwFormatContext(workDir);

    GWContextConfig config;
    config.blockSize = 10;
    config.numBlocks = 100;

    fs =  gwCreateContext(workDir, &config);

    gwFile file = gwOpenFile(fs, "/test1", GW_CREAT|GW_RDWR);

    gwWrite(fs, file, input, sizeof(input));

    gwSeek(fs, file, 10, SEEK_SET);

    int len = gwRead(fs, file, buffer, 20);
    buffer[len] = '\0';

    printf("Read From Gopherwood %s", buffer);

    gwCloseFile(fs, file);

    gwDestroyContext(fs);

    return 0;
}