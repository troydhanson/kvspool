#include <glob.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include "kvspool.h"
#include "utarray.h"
#include "utils.h"

char *base = "mybase";


/******************************************************************************
 * the test itself
 *****************************************************************************/
int main(int argc, char *argv[]) {

  char *dir = mktmpdir();
  int rc;

  void *sp = kv_spoolwriter_new(dir,base);
  if (!sp) exit(-1);

  void *set = kv_set_new();
  kv_adds(set, "hello", "again");
  kv_adds(set, "second", "life");
  kv_spool_write(sp,set);
  kv_adds(set, "hello", "there");
  kv_adds(set, "second", "world");
  kv_spool_write(sp,set);
  kv_spoolwriter_free(sp);

  /* now try reading the spool */
  sp = kv_spoolreader_new(dir,base);
  while ( (rc=kv_spool_read(sp, set, 0)) == 1) {
    printf("reader read frame, base %s\n", kv_set_base(set));
  }
  kv_spoolreader_free(sp);

  /* repeat the test for wildcard base */
  printf("wildcard base\n");
  sp_reset(dir,base);
  sp = kv_spoolreader_new(dir,"");
  while ( (rc=kv_spool_read(sp, set, 0)) == 1) {
    printf("reader read frame, base %s\n", kv_set_base(set));
  }
  kv_spoolreader_free(sp);

  kv_set_free(set);
  return 0;
}
