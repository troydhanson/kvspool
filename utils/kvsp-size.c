#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "kvspool.h"
#include "utstring.h"

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] -s <max> spool [ spool ... ]\n", prog);
  fprintf(stderr, "       <max> is directory max size e.g. 1G (units KMGT)\n");
  fprintf(stderr, "       Note: this command makes the limit persistent\n");
  exit(-1);
}
 
int main(int argc, char * argv[]) {
  int opt,verbose=0;
  long dirmax=0;
  /* input spool */
  char *dir=NULL,unit,*sz="10GB";
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
              case 't': case 'T': break;
              case 'g': case 'G': break;
              case 'm': case 'M': break;
              case 'k': case 'K': break;
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
  kv_spool_options.dir_max = dirmax;

  while (optind < argc) {
    dir = argv[optind++];

    utstring_clear(s);
    utstring_printf(s,"%s/limits", dir);
    char *p = utstring_body(s);
    FILE *f = fopen(p, "w");
    if (f == NULL) {
      fprintf(stderr,"cannot open %s: %s\n", p, strerror(errno));
      continue;
    }
    fprintf(f, "%s", sz);
    fclose(f);
    sp_attrition(dir);
  }

 done:
  utstring_free(s);
  return 0;
}

