#include <stdio.h>
#include <time.h>
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
#include "utstring.h"

int verbose;

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] spool\n", prog);
  exit(-1);
}

void dir_status(char*dir) {
  int fd=-1,sc,i;
  char **f, *file;
  time_t now, elapsed;
  UT_array *files;
  utarray_new(files,&ut_str_icd);
  struct stat sb;
  uint32_t sz, spsz;

  kv_stat_t stats;
  sc = kv_stat(dir,&stats);
  if (sc == -1) {
    printf("kv_stat error in %s\n", dir);
    goto done; 
  }
  UT_string *s;
  utstring_new(s);
  utstring_printf(s,"%3u%% ", stats.pct_consumed);
  long lz = stats.spool_sz;
  char *unit = "";
  if      (lz > 1024*1024*1024){unit="gb "; lz/=(1024*1024*1024); }
  else if (lz > 1024*1024)     {unit="mb "; lz/=(1024*1024);      }
  else if (lz > 1024)          {unit="kb "; lz/=(1024);           }
  else                         {unit="b ";                        }
  utstring_printf(s,"%10lu%s", (long)lz, unit);

  unit="";
  now = time(NULL);
  elapsed =  now - stats.last_write;
  if      (elapsed > 60*60*24) {unit="days";  elapsed/=(60*60*24);}
  else if (elapsed > 60*60)    {unit="hours"; elapsed/=(60*60);   }
  else if (elapsed > 60)       {unit="mins";  elapsed/=(60);      }
  else if (elapsed >= 0)       {unit="secs";                      }
  if (stats.last_write == 0)   utstring_printf(s,"%10snever","");
  else utstring_printf(s,"%10lu%s", (long)elapsed, unit);
  printf("%s\n", utstring_body(s));
  utstring_free(s);

  /* the rest is for verbose output */
  if (!verbose) goto done;
  printf("\n\n");
  if (sp_readdir(dir, ".sr", files) == -1) goto done;
  f = NULL;
  while ( (f=(char**)utarray_next(files,f))) {
    file = *f;
    printf("\t%s ",file);
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
}
 
int main(int argc, char * argv[]) {
  char *dir;
  int opt;

  while ( (opt = getopt(argc, argv, "v+")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      default: usage(argv[0]); break;
    }
  }
  if (optind >= argc) usage(argv[0]);

  while(optind < argc) {
    dir = argv[optind++];
    printf("%-40s ", dir);
    dir_status(dir);
  }

  return 0;
}

