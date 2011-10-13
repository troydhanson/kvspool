#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "kvspool.h"
#include "utarray.h"

typedef struct {
  char *base;
  void *sp;
} ospool_base_t;

typedef struct {
  char *dir;
  UT_array /* of ospool_base_t */ basev;
} ospool_t;

/* plumbing for ospool_base_t array */
void ospool_base_ini(void *_ospb) {
  ospool_base_t *ospb = (ospool_base_t*)_ospb;
  ospb->base=NULL;
  ospb->sp=NULL;
}
void ospool_base_cpy(void *_dst, const void *_src) {
  ospool_base_t *dst = (ospool_base_t*)_dst;
  ospool_base_t *src = (ospool_base_t*)_src;
  dst->base = src->base ? strdup(src->base) : NULL;
  dst->sp = src->sp;
}
void ospool_base_fin(void *_ospb) {
  ospool_base_t *ospb = (ospool_base_t*)_ospb;
  if (ospb->base) free(ospb->base);
  /* we don't release osbp->sp because its shared */
}

UT_icd ospool_base_icd = {sizeof(ospool_base_t),ospool_base_ini,ospool_base_cpy,ospool_base_fin};
 
/* plumbing for ospool_t array */
void ospool_ini(void *_osp) {
  ospool_t *osp = (ospool_t*)_osp;
  osp->dir = NULL;
  utarray_init(&osp->basev, &ospool_base_icd);
}
void ospool_cpy(void *_dst, const void *_src) {
  ospool_t *dst = (ospool_t*)_dst;
  ospool_t *src = (ospool_t*)_src;
  dst->dir = src->dir ? strdup(src->dir) : NULL;
  utarray_init(&dst->basev, &ospool_base_icd);
  utarray_concat(&dst->basev, &src->basev);
}
void ospool_fin(void *_osp) {
  ospool_t *osp = (ospool_t*)_osp;
  if (osp->dir) free(osp->dir);
  utarray_done(&osp->basev);
}

UT_icd ospool_icd = {sizeof(ospool_t),ospool_ini,ospool_cpy,ospool_fin};

 
void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] -s spool [-d base] <dstdir> ...\n", prog);
  exit(-1);
}
 
int main(int argc, char * argv[]) {
  int opt,verbose=0,dirmax=0,filemax=0;
  ospool_t *osp;
  void *set;
  /* input spool */
  char *dir=NULL;
  char *base=NULL;
  void *sp;

  set = kv_set_new();
  UT_array *ospoolv;
  utarray_new(ospoolv, &ospool_icd);
 
  while ( (opt = getopt(argc, argv, "v+D:F:b:s:")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'D': dirmax = atoi(optarg); if (dirmax==0) usage(argv[0]); break;
      case 'F': filemax = atoi(optarg); if (filemax==0) usage(argv[0]); break;
      case 'b': base = strdup(optarg); break;
      case 's': dir = strdup(optarg); break;
      default: usage(argv[0]); break;
    }
  }
  if (dir==NULL) usage(argv[0]);
  sp = kv_spoolreader_new(dir,base);
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
      /* do we already have a spool handle for this base? */
      char *base = kv_set_base(set);
      ospool_base_t *ospb=NULL;
      while ( (ospb=(ospool_base_t*)utarray_next(&osp->basev,ospb))) {
        if (strcmp(ospb->base,base)) continue;
        break; /* found an open spool handle */
      }
      if (ospb==NULL) { /* didn't have a spool handle for base? */
        utarray_extend_back(&osp->basev);
        ospb = (ospool_base_t*)utarray_back(&osp->basev);
        ospb->base = strdup(base);
        ospb->sp = kv_spoolwriter_new(osp->dir,ospb->base);
        if (!ospb->sp) {
          fprintf(stderr, "failed to open output spool %s base %s\n",dir,base);
          goto done;
        }
      }
      kv_spool_write(ospb->sp,set);
    }
  }

 done:
  kv_set_free(set);
  osp=NULL;
  /* deep free the spoolwriter handles */
  while ( (osp=(ospool_t*)utarray_next(ospoolv,osp))) {
    ospool_base_t *ospb=NULL;
    while ( (ospb=(ospool_base_t*)utarray_next(&osp->basev,ospb))) {
      kv_spoolwriter_free(ospb->sp);
    }
  }
  utarray_free(ospoolv);
  return 0;
}

