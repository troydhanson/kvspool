#include <glob.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include "kvspool.h"
#include "utarray.h"
#include "utils.h"

char *base = "abe";
char *base2 = "bob";

int main(int argc, char *argv[]) {
  int i,rc;

  char *dir = mktmpdir();

  void *sp = kv_spoolwriter_new(dir,base);
  if (!sp) exit(-1);

  void *set = kv_set_new();
  kv_adds(set, "hello", "again");
  kv_adds(set, "second", "life");
  kv_spool_write(sp,set);


  /* create a second spoolwriter in the same directory. 
   * because the first spoolwriter has a lock on the file it contains,
   * this spoolwriter should create its own file, rather than append. */
  void *sp2 = kv_spoolwriter_new(dir,base);
  if (!sp2) exit(-1);
  /* replace a value in the set, spool the set out. */
  printf("spooling second frame\n");
  kv_adds(set, "second", "new spool file");
  kv_spool_write(sp2,set);

  /* spool a set out to another base. this should make the third spoolfile */
  void *sp3 = kv_spoolwriter_new(dir,base2);
  if (!sp3) exit(-1);
  kv_spool_write(sp3,set);

  /* statdir now should have two records (base and base2) both 0% consumed */
  kv_stat_t *stats;
  rc = kv_stat(dir,NULL,&stats);
  printf("%d stat records:\n", rc);
  for(i=0; i < rc; i++) printf("%s %d\n", stats[i].base, stats[i].pct_consumed);
  if (stats) free(stats);

  /* consume some */
  void *sp4 = kv_spoolreader_new(dir,base);  kv_spool_read(sp4,set,0); /* 50% */
  void *sp5 = kv_spoolreader_new(dir,base2); kv_spool_read(sp5,set,0); /*100% */

  /* view the stats now */
  rc = kv_stat(dir,NULL,&stats);
  printf("%d stat records:\n", rc);
  for(i=0; i < rc; i++) printf("%s %d\n", stats[i].base, stats[i].pct_consumed);
  if (stats) free(stats);

  //printf("dir is %s\n",dir); sleep(100);

  kv_spoolwriter_free(sp);
  kv_spoolwriter_free(sp2);
  kv_spoolwriter_free(sp3);
  kv_spoolreader_free(sp4);
  kv_spoolreader_free(sp5);
  kv_set_free(set);

  /* verify spool has updated according to expectation */
  scan_spool(1);

  return 0;
}
