#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#include "gopherwood/gopherwood.h"
static gopherwoodFS fs;

int main(int argc, char *argv[])
{
    char buffer[]= "hello, world!";

    fs =  gwCreateContext("/tmp/gopherwood");

    gwFile file = gwOpenFile(fs, "/test1", GW_CREAT|GW_RDWR);

    gwWrite(fs, file, buffer, 10);

    return 0;
}