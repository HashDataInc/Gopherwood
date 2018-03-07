#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "../../src/client/gopherwood.h"

static gopherwoodFS fs;
char workDir[] = "/tmp/gopherwood";

int main(int argc, char *argv[])
{
    char buffer[]= "hello, world!";

    gwFormatContext(workDir);

    fs =  gwCreateContext(workDir);

    gwFile file = gwOpenFile(fs, "/test1", GW_CREAT|GW_RDWR);

    gwWrite(fs, file, buffer, sizeof(buffer));

    gwSeek(fs, file, 7, SEEK_SET);

    gwRead(fs, file, buffer, 5);

    printf("!!!%s!!!", buffer);

    gwCloseFile(fs, file);

    return 0;
}