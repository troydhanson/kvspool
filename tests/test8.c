#include <stdio.h>
#include "kvspool_internal.h"
#include "utarray.h"


int main(int argc, char * argv[] ) {
  char *f, **p;
  UT_array *files;
  utarray_new(files,&ut_str_icd);

  f="spool.1234567.111-000.sp"; utarray_push_back(files,&f);
  f="spool.1234567.222-001.sp"; utarray_push_back(files,&f);
  f="spool.1234567.222-002.sp"; utarray_push_back(files,&f);
  f="spool.1234567.333-002.sp"; utarray_push_back(files,&f);

  sp_keep_maxseq(files);
  p=NULL;
  while ( (p=(char**)utarray_next(files,p))) {
    printf("%s\n",*p);
  }
  utarray_free(files);
  return 0;
}
