#include <glob.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include "kvspool.h"
#include "utarray.h"
#include "utils.h"

int main(int argc, char *argv[]) {

  char *dir = mktmpdir();

  void *sp = kv_spoolwriter_new(dir);
  if (!sp) exit(-1);

  void *set = kv_set_new();
  kv_adds(set, "hello", "again");
  kv_adds(set, "second", "life");
  kv_spool_write(sp,set);

  /* scan spool to validate expected file creation */
  scan_spool(0);

  /* replace a value in the set, spool the set out. it should append
   * to the spool file previously created. */
  printf("spooling second frame\n");
  kv_adds(set, "second", "time");
  kv_spool_write(sp,set);
  kv_spoolwriter_free(sp);
  kv_set_free(set);

  /* verify spool has updated according to expectation */
  scan_spool(1);

  return 0;
}
