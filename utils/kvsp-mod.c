#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "kvspool.h"
#include "utarray.h"
#include "utstring.h"

int verbose=0;
char *base = NULL;
char *dir = NULL;
char *conf = NULL;
char *odir = ".";
int block=1;
UT_array *keys;

typedef struct {
  char *base;
  void *sp;
  UT_hash_handle hh;
} osp_t;

osp_t *ospv = NULL;

void usage(char *exe) {
  fprintf(stderr,"usage: %s [-v] [-b base] -k <key> "
                 "[-k <key>...] [-o outdir] <spooldir>\n", exe);
  exit(-1);
}

void *get_output_spool(char *base) {
  osp_t *osp;
  HASH_FIND_STR(ospv,base,osp);
  if (!osp) {
    osp = malloc(sizeof(*osp)); if (!osp) goto done;
    osp->sp = kv_spoolwriter_new(odir,base);
    if (osp->sp == NULL) { free(osp); osp=NULL; goto done;}
    osp->base = strdup(base);
    HASH_ADD_KEYPTR(hh,ospv,osp->base,strlen(base),osp);
  }
 done:
  return osp ? osp->sp : NULL;
}

int main(int argc, char *argv[]) {

  kv_t *kv;
  int opt,rc;
  osp_t *osp, *tmp;
  char *exe = argv[0];
  utarray_new(keys,&ut_str_icd);

  while ( (opt = getopt(argc, argv, "b:v+k:c:n:o:")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'b': base=strdup(optarg); break;
      case 'c': conf=strdup(optarg); break;
      case 'o': odir=strdup(optarg); break;
      case 'k': utarray_push_back(keys,&optarg); break;
      default: usage(exe); break;
    }
  }
  if (optind < argc) dir=argv[optind++];
  else usage(exe);

  void *sp = kv_spoolreader_new(dir,base);
  if (!sp) exit(-1);

  void *set = kv_set_new();
  while ( (rc=kv_spool_read(sp,set,block)) > 0) {
    /* calculate hash value of keys */
    char *key, *val, **k=NULL;
    while ( (k=(char**)utarray_next(keys,k))) {
      key = *k;
      kv = kv_get(set, key);
      if (!kv) {if (verbose) fprintf(stderr,"no such key: %s; skipping\n", key); continue;}
      /* replace the value with a hash of the value */
      unsigned hv=0, vlen=kv->vlen; val = kv->val;
      while(vlen--) hv = hv * 33 + *val++; 
      // we could write it in binary but we change it to text
      //kv_add(set, kv->key, kv->klen, (void*)&hv, sizeof(hv));
      char hv_str[10];
      snprintf(hv_str,sizeof(hv_str),"%u",hv);
      kv_add(set, kv->key, kv->klen, hv_str, strlen(hv_str));
    }
    char *sbase = kv_set_base(set);
    osp = get_output_spool(sbase);
    if (kv_spool_write(osp, set) != 0) {printf("output error\n"); break; }
  }

  /* clean up */
  HASH_ITER(hh,ospv,osp,tmp) {
    HASH_DEL(ospv, osp);
    free(osp->base);
    kv_spoolwriter_free(osp->sp);
  }
  kv_spoolreader_free(sp);
  kv_set_free(set);
  utarray_free(keys);
  return 0;
}
