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


/******************************************************************************
 * the test itself
 *****************************************************************************/
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

  /* replace a value in the set, spool the set out. it should append
   * to the spool file previously created. */
  printf("spooling second frame\n");
  kv_adds(set, "second", "time");
  kv_spool_write(sp,set);
  kv_spoolwriter_free(sp);

  /* verify spool has updated according to expectation */
  scan_spool(0);

  printf("clear set\n");
  kv_set_clear(set);
  /* loop over them */
  kv_t *kv = NULL;
  while ( (kv = kv_next(set, kv))) {
    printf("key [%.*s], val [%.*s]\n", kv->klen, kv->key, kv->vlen, kv->val);
  }

  printf("reading from spool\n");
  /* now try reading the spool */
  sp = kv_spoolreader_new(dir,base);
  int rc;
  while ( (rc=kv_spool_read(sp, set, 0)) == 1) {
    printf("reader read frame:\n");
    kv = NULL;
    while ( (kv = kv_next(set, kv))) {
      printf(" key [%.*s], val [%.*s]\n", kv->klen, kv->key, kv->vlen, kv->val);
    }
  }
  printf("kv_spool_read returned %d\n", rc);
  //kv_spoolreader_free(sp);

  /* now reset it and read again */
  printf("resetting the spool directory for another round of reading\n");
  sp_reset(dir,base);
  //sp = kv_spoolreader_new(dir,base);
  while ( (rc=kv_spool_read(sp, set, 0)) == 1) {
    printf("reader read frame:\n");
    kv = NULL;
    while ( (kv = kv_next(set, kv))) {
      printf(" key [%.*s], val [%.*s]\n", kv->klen, kv->key, kv->vlen, kv->val);
    }
  }
  printf("kv_spool_read returned %d\n", rc);
  kv_spoolreader_free(sp);

  kv_set_free(set);
  scan_spool(1);
  return 0;
}
