#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include "kvspool.h"
#include "shr.h"
#include "utstring.h"

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] -s <max> spool [ spool ... ]\n", prog);
  fprintf(stderr, "       <max> is directory max size e.g. 1G (units KMGT)\n");
  exit(-1);
}
 
int main(int argc, char * argv[]) {
  int opt,verbose=0, rc = -1, sc;
  char path[PATH_MAX];
  long dirmax=10*1024*1024;
  /* input spool */
  char *dir=NULL,unit,*sz;
  UT_string *s;
  utstring_new(s);

  while ( (opt = getopt(argc, argv, "v+s:")) != -1) {
    switch (opt) {
      default: usage(argv[0]); break;
      case 'v': verbose++; break;
      case 's': 
         sz = strdup(optarg);
         switch (sscanf(sz, "%ld%c", &dirmax, &unit)) {
           case 2: /* check unit */
            switch (unit) {
              case 't': case 'T': dirmax *= 1024; /* FALLTHRU */
              case 'g': case 'G': dirmax *= 1024; /* FALLTHRU */
              case 'm': case 'M': dirmax *= 1024; /* FALLTHRU */
              case 'k': case 'K': dirmax *= 1024; /* FALLTHRU */
              case '\r': case '\n': case ' ': case '\t': break;
              default: usage(argv[0]); break;
            }
           case 1: /* just a number in bytes */ break;
           default: usage(argv[0]); break;
         }
         break;
    }
  }
  if (optind >= argc) usage(argv[0]);
  if (!dirmax) usage(argv[0]);
  umask(0);

  while (optind < argc) {
    dir = argv[optind++];
    snprintf(path, PATH_MAX, "%s/%s", dir, "data");
    sc = shr_init(path, dirmax, SHR_KEEPEXIST|SHR_MESSAGES|SHR_DROP);
    if (sc < 0) goto done;
    sc = chmod(path, 0666);
    if (sc < 0) {
      fprintf(stderr, "chmod: %s\n", strerror(errno));
    }
  }

  rc = 0;

 done:
  utstring_free(s);
  return rc;
}

