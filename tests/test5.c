#include <glob.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include "kvspool.h"
#include "utarray.h"
#include "utils.h"

char *base = "spool";

int main(int argc, char *argv[]) {

  char *dir = mktmpdir();

  void *sp = kv_spoolwriter_new(dir,base);
  if (!sp) exit(-1);

  void *set = kv_set_new();
  kv_adds(set, "hello", "again");
  kv_adds(set, "second", "life");
  kv_spool_write(sp,set);

  /* scan spool to validate expected file creation */
  scan_spool(0);

  /* create a second spoolwriter in the same directory. 
   * because the first spoolwriter has a lock on the file it contains,
   * this spoolwriter should create its own file, rather than append. */
  void *sp2 = kv_spoolwriter_new(dir,base);
  if (!sp2) exit(-1);
  /* replace a value in the set, spool the set out. */
  printf("spooling second frame\n");
  kv_adds(set, "second", "new spool file");
  kv_spool_write(sp2,set);
  kv_spoolwriter_free(sp);
  kv_spoolwriter_free(sp2);
  kv_set_free(set);

  /* verify spool has updated according to expectation */
  scan_spool(1);

  return 0;
}
