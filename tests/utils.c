#include <glob.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include "kvspool.h"
#include "utarray.h"

/******************************************************************************
 * boilerplate utilities for test
 *****************************************************************************/
static char *wild = "*";
static char dir[100];
static void cleanup(void) { chdir("/tmp"); if (*dir) rmdir(dir); }
char* mktmpdir(void) {
  snprintf(dir,sizeof(dir),"/tmp/test.%d", (int)getpid());
  mkdir(dir,0777);
  atexit(cleanup);
  return dir;
}
/* change digits to '-', used to normalize output for automated tests */
static char *blot(char*c) {
  char *s;
  for(s=c; *s != '\0'; s++) { if ((*s >= '0') && (*s <= '9')) *s='*'; }
  return c;
}

static int strsort(const void *_a, const void *_b) {
  char *a = *(char**)_a;
  char *b = *(char**)_b;
  return strcmp(a,b);
}
typedef struct {
  int file_idx;
  int file_len;
} file_xtra;
static UT_icd xtra_icd = {sizeof(file_xtra),NULL,NULL,NULL};
static int xtrasort(const void *_a, const void *_b) {
  file_xtra *a = (file_xtra*)_a;
  file_xtra *b = (file_xtra*)_b;
  return (a->file_len - b->file_len);
}
void scan_spool(int do_unlink) {
  int i;
  glob_t g;
  struct stat sb;
  UT_array *files;
  UT_array *xtras;
  utarray_new(files,&ut_str_icd);
  utarray_new(xtras,&xtra_icd);
  char **f, *file;
  file_xtra x, *xp;

  if (chdir(dir) == -1) exit(-1);
  glob(wild, 0, NULL, &g);
  for(i=0; i < g.gl_pathc; i++) {
    utarray_push_back(files, &g.gl_pathv[i]);
  }
  utarray_sort(files, strsort);

  f=NULL;
  while ( (f=(char**)utarray_next(files,f))) {
    file = *f;
    stat(file,&sb);
    if (do_unlink) unlink(file);
    x.file_idx = utarray_eltidx(files,f);
    x.file_len = sb.st_size;
    utarray_push_back(xtras, &x);
  }
  utarray_sort(xtras, xtrasort);
  xp=NULL;
  while ( (xp=(file_xtra*)utarray_next(xtras,xp))) {
    f = (char**)utarray_eltptr(files,xp->file_idx);
    file = *f;
    printf("file %s, len %d\n", blot(file),xp->file_len);
  }
  globfree(&g);
  utarray_free(files);
  utarray_free(xtras);
}

