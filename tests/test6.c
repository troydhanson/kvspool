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

  /* customize the spool options */
  kv_spool_options.dir_max = 3*1024*1024; /* 3 mb */

  char *dir = mktmpdir();

  void *sp = kv_spoolwriter_new(dir,base);
  if (!sp) exit(-1);

  void *set = kv_set_new();
  kv_adds(set, "hello", "again");
  kv_adds(set, "second", "life");
  kv_spool_write(sp,set);

  /* scan spool to validate expected file creation */
  scan_spool(0);

  int i, num_spoolouts = 20000; 
  /* one set is about 54 bytes so it takes that many to exceed the 1mb 
     spool max size and induce creation of a second spool file */
  printf("spooling %d more frames\n", num_spoolouts);
  for(i=0; i<num_spoolouts; i++) kv_spool_write(sp,set);
  scan_spool(0); 

  /* a third spool */
  printf("spooling %d more frames\n", num_spoolouts);
  for(i=0; i<num_spoolouts; i++) kv_spool_write(sp,set);
  scan_spool(0); 

  /* a fourth spool.. induces some attrition */
  printf("spooling %d more frames\n", num_spoolouts);
  for(i=0; i<num_spoolouts; i++) kv_spool_write(sp,set);
  scan_spool(1); 

  kv_spoolwriter_free(sp);
  kv_set_free(set);
  return 0;
}
