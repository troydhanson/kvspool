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
  double pi = 3.14159;

  char *dir = mktmpdir();

  void *sp = kv_spoolwriter_new(dir,base);
  if (!sp) exit(-1);

  void *set = kv_set_new();
  kv_adds(set, "hello", "again");
  kv_adds(set, "second", "life");
  kv_addt(set, "vnull", 'b', NULL, 0);
  kv_addt(set, "adouble", 'f', &pi, sizeof(double));
  kv_spool_write(sp,set);
  kv_set_free(set);

  kv_spoolwriter_free(sp);

  /* scan spool to validate expected file creation */
  scan_spool(1);

  return 0;
}
