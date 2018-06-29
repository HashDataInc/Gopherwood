/**
 * User: Houliang Qi
 * Date: 3/24/18
 * Time: 11:12 PM
 */

#include "dbtproj.h"

gopherwoodFS dbtproj::gwFS;
int64_t dbtproj::totalReadTime;

void dbtproj::formatGopherwood() {
    char workDir[] = WORK_DIR;
    gwFormatContext(workDir);

}