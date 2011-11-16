#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "kvspool.h"
#include "utarray.h"

typedef struct {
  char *dir;
  void *sp;
} ospool_t;

/* plumbing for ospool_t array */
void ospool_ini(void *_osp) {
  ospool_t *osp = (ospool_t*)_osp;
  osp->dir = NULL;
  osp->sp = NULL;
}
void ospool_cpy(void *_dst, const void *_src) {
  ospool_t *dst = (ospool_t*)_dst;
  ospool_t *src = (ospool_t*)_src;
  dst->dir = src->dir ? strdup(src->dir) : NULL;
  dst->sp = src->sp;
}
void ospool_fin(void *_osp) {
  ospool_t *osp = (ospool_t*)_osp;
  if (osp->dir) free(osp->dir);
}

UT_icd ospool_icd = {sizeof(ospool_t),ospool_ini,ospool_cpy,ospool_fin};

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] -s spool <dstdir> ...\n", prog);
  exit(-1);
}
 
int main(int argc, char * argv[]) {
  int opt,verbose=0,dirmax=0,filemax=0;
  ospool_t *osp;
  void *set;
  /* input spool */
  char *dir=NULL;
  void *sp;

  set = kv_set_new();
  UT_array *ospoolv;
  utarray_new(ospoolv, &ospool_icd);
 
  while ( (opt = getopt(argc, argv, "v+D:F:s:")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'D': dirmax = atoi(optarg); if (dirmax==0) usage(argv[0]); break;
      case 'F': filemax = atoi(optarg); if (filemax==0) usage(argv[0]); break;
      case 's': dir = strdup(optarg); break;
      default: usage(argv[0]); break;
    }
  }
  if (dir==NULL) usage(argv[0]);
  sp = kv_spoolreader_new(dir);
  if (sp == NULL) {
      fprintf(stderr, "failed to open input spool %s\n", dir);
      goto done;
  }

  while (optind < argc) {
    utarray_extend_back(ospoolv);
    osp = (ospool_t*)utarray_back(ospoolv);
    osp->dir = strdup(argv[optind++]);
  }

  while (kv_spool_read(sp,set,1) == 1) {
    osp=NULL;
    while ( (osp=(ospool_t*)utarray_next(ospoolv,osp))) {
      if (osp->sp ==NULL) { /* do lazy open */
        osp->sp = kv_spoolwriter_new(osp->dir);
        if (!osp->sp) {
          fprintf(stderr, "failed to open output spool %s\n",dir);
          goto done;
        }
      }
      kv_spool_write(osp->sp,set);
    }
  }

 done:
  kv_set_free(set);
  osp=NULL;
  /* free the spoolwriter handles */
  while ( (osp=(ospool_t*)utarray_next(ospoolv,osp))) {
    kv_spoolwriter_free(osp->sp);
  }
  utarray_free(ospoolv);
  return 0;
}

