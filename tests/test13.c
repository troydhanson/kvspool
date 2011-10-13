#include <glob.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include "kvspool.h"
#include "utarray.h"
#include "utils.h"

char *base = "exeter";
char *base2 = "leeds";

int main(int argc, char *argv[]) {
  int i,rc;

  kv_stat_t *stats;
  char *dir = mktmpdir();

  /* shouldn't have any stats yet */
  rc = kv_stat(dir,NULL,&stats);
  printf("%d stat records:\n", rc);
  for(i=0; i < rc; i++) printf("%s %d\n", stats[i].base, stats[i].pct_consumed);
  if (stats) free(stats);

  void *sp = kv_spoolwriter_new(dir,base);
  if (!sp) exit(-1);

  /* should have one stats record, an empty spool (by definition 100% consumed) */
  rc = kv_stat(dir,NULL,&stats);
  printf("%d stat records:\n", rc);
  for(i=0; i < rc; i++) printf("%s %d\n", stats[i].base, stats[i].pct_consumed);
  if (stats) free(stats);

  void *set = kv_set_new();
  kv_adds(set, "hello", "again");
  kv_adds(set, "second", "life");
  kv_spool_write(sp,set); /* record 1 */
  kv_spool_write(sp,set); /* record 2 */
  kv_spool_write(sp,set); /* record 3 */

  /* should have one stats record, now 0% consumed */
  rc = kv_stat(dir,NULL,&stats);
  for(i=0; i < rc; i++) printf("%s %d\n", stats[i].base, stats[i].pct_consumed);
  if (stats) free(stats);

  void *sp2 = kv_spoolreader_new(dir,base); 
  kv_spool_read(sp2,set,0); /* 33% */

  /* should have one stats record, now 33% consumed */
  rc = kv_stat(dir,NULL,&stats);
  for(i=0; i < rc; i++) printf("%s %d\n", stats[i].base, stats[i].pct_consumed);
  if (stats) free(stats);

  kv_spool_read(sp2,set,0); /* 66% */

  /* should have one stats record, now 66% consumed */
  rc = kv_stat(dir,NULL,&stats);
  for(i=0; i < rc; i++) printf("%s %d\n", stats[i].base, stats[i].pct_consumed);
  if (stats) free(stats);

  kv_spool_write(sp,set); /* record 4 */

  /* we still only consumed two records (but there are now 4) so 50% */
  rc = kv_stat(dir,NULL,&stats);
  for(i=0; i < rc; i++) printf("%s %d\n", stats[i].base, stats[i].pct_consumed);
  if (stats) free(stats);

  kv_spool_read(sp2,set,0); /* 75% */

  /* we consumed three records (of 4) so 75% */
  rc = kv_stat(dir,NULL,&stats);
  for(i=0; i < rc; i++) printf("%s %d\n", stats[i].base, stats[i].pct_consumed);
  if (stats) free(stats);

  kv_spool_read(sp2,set,0); /* 100% */

  /* should have one stats record, now 100% consumed */
  rc = kv_stat(dir,NULL,&stats);
  for(i=0; i < rc; i++) printf("%s %d\n", stats[i].base, stats[i].pct_consumed);
  if (stats) free(stats);

  kv_spoolwriter_free(sp);
  kv_spoolreader_free(sp2);
  kv_set_free(set);

  /* verify spool has updated according to expectation */
  scan_spool(1);

  return 0;
}
