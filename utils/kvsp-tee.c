#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <pcre.h>
#include "kvspool_internal.h"
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
  fprintf(stderr, "usage: %s [-v] [-k key -r regex] -s spool <dstdir> ...\n", prog);
  exit(-1);
}

#define OVECSZ 30 /* must be multiple of 3 */
int keep_record(void *set, char *key, pcre *re) {
  int rc, ovec[OVECSZ];
  if (!key) return 1;
  if (!re) return 1;

  /* does set contain key */
  kv_t *kv = kv_get(set, key);
  if (kv == NULL) return 0; 

  /* does value of key match regex */
  rc = pcre_exec(re, NULL, kv->val, kv->vlen, 0, 0, ovec, OVECSZ);
  return (rc > 0) ? 1 : 0;
}
 
int main(int argc, char * argv[]) {
  char *key=NULL, *regex=NULL;
  pcre *re;
  int opt,verbose=0,raw=0;
  ospool_t *osp;
  void *set;
  /* input spool */
  char *dir=NULL;
  void *sp;

  set = kv_set_new();
  UT_array *ospoolv;
  utarray_new(ospoolv, &ospool_icd);
 
  while ( (opt = getopt(argc, argv, "v+s:k:r:W")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 's': dir = strdup(optarg); break;
      case 'k': key = strdup(optarg); break;
      case 'r': regex = strdup(optarg); break;
      case 'W': raw = 1; break;
      default: usage(argv[0]); break;
    }
  }
  if (raw) fprintf(stderr, "-W (raw mode) is deprecated, using regular mode\n");
  if (dir==NULL) usage(argv[0]);
  if (key && regex) {
      const char *err;
      int off; 
      re = pcre_compile(regex, 0, &err, &off, NULL);
      if (!re) {
        fprintf(stderr, "error in regex\n");
        goto done;
      }
  }
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
    if (!keep_record(set,key,re)) continue;
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

