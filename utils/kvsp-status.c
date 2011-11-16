#include <stdio.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "kvspool.h"
#include "kvspool_internal.h"
#include "utarray.h"

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] spool\n", prog);
  exit(-1);
}
 
int main(int argc, char * argv[]) {
  int opt,verbose=0,fd=-1,sc,i;
  char *dir=NULL, **f, *file;
  UT_array *files;
  utarray_new(files,&ut_str_icd);
  struct stat sb;
  uint32_t sz, spsz;

  while ( (opt = getopt(argc, argv, "v+")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      default: usage(argv[0]); break;
    }
  }
  if (optind >= argc) usage(argv[0]);
  dir = argv[optind++];

  kv_stat_t stats;
  sc = kv_stat(dir,&stats);
  if (sc == -1) {
    printf("kv_stat error in %s\n", dir);
    goto done; 
  }
  printf("%3u%%\n", stats.pct_consumed);

  /* the rest is for verbose output */
  if (!verbose) goto done;
  printf("\n\n");
  if (sp_readdir(dir, ".sr", files) == -1) goto done;
  f = NULL;
  while ( (f=(char**)utarray_next(files,f))) {
    file = *f;
    printf("%s ",file);
    if ( (fd=open(file,O_RDONLY)) == -1) {
      perror("cannot open");
      goto done;
    }
    if (read(fd,&sz,sizeof(sz)) != sizeof(sz)) {
      perror("cannot open");
      close(fd);
      goto done;
    }
    close(fd);
    file[strlen(file)-1]='p';
    if (stat(file,&sb) == -1) spsz = 0;
    else spsz = sb.st_size;
    /* ignore the spool preamble */
    if (spsz) spsz-=8;
    sz -= 8;
    printf("%u/%u (%2.2f%%)\n", sz, spsz, (spsz?(sz*100.0/spsz):0));
  }

 done:
  utarray_free(files);
  return 0;
}

