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
unsigned n=2;
UT_array *keys;

typedef struct {
  char *base;
  void *sp;
  UT_hash_handle hh;
} osp_t;

osp_t **ospv;

void usage(char *exe) {
  fprintf(stderr,"usage: %s [-v] [-b base] -n <num> -k <key> "
                 "[-k <key>...] [-o outdir] <spooldir>\n", exe);
  exit(-1);
}

void *get_output_spool(unsigned hv, char *base) {
  osp_t *osp;
  HASH_FIND_STR(ospv[hv%n],base,osp);
  if (!osp) {
    osp = malloc(sizeof(*osp)); if (!osp) goto done;
    osp->base = strdup(base);
    UT_string *suffixed_base; utstring_new(suffixed_base);
    utstring_printf(suffixed_base, "%s-%u", base, (unsigned)(hv%n));
    osp->sp = kv_spoolwriter_new(odir,utstring_body(suffixed_base));
    utstring_free(suffixed_base);
    if (osp->sp == NULL) { free(osp); return NULL;}
    HASH_ADD_KEYPTR(hh,ospv[hv%n],osp->base,strlen(base),osp);
  }
 done:
  return osp ? osp->sp : NULL;
}

int main(int argc, char *argv[]) {

  kv_t *kv;
  int opt,rc,i;
  osp_t *osp, *tmp;
  char *exe = argv[0];
  utarray_new(keys,&ut_str_icd);

  while ( (opt = getopt(argc, argv, "b:v+k:c:n:o:")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'b': base=strdup(optarg); break;
      case 'c': conf=strdup(optarg); break;
      case 'o': odir=strdup(optarg); break;
      case 'n': n=atoi(optarg); break;
      case 'k': utarray_push_back(keys,&optarg); break;
      default: usage(exe); break;
    }
  }
  if (optind < argc) dir=argv[optind++];
  else usage(exe);

  ospv = calloc(n, sizeof(void*));

  void *sp = kv_spoolreader_new(dir,base);
  if (!sp) exit(-1);

  void *set = kv_set_new();
 next_frame:
  while ( (rc=kv_spool_read(sp,set,block)) > 0) {
    /* calculate hash value of keys */
    unsigned hv=0, vlen=0; char *val, *key, **k=NULL;
    while ( (k=(char**)utarray_next(keys,k))) {
      key = *k;
      kv = kv_get(set, key);
      if (!kv) {printf("no such key: %s; skipping\n", key); goto next_frame;}
      vlen = kv->vlen; val = kv->val;
      while(vlen--) hv = hv * 33 + *val++; 
    }
    char *sbase = kv_set_base(set);
    osp = get_output_spool(hv,sbase);
    if (kv_spool_write(osp, set) != 0) {printf("output error\n"); break; }
  }

  /* clean up */
  for(i = 0; i < n; i++) {
    HASH_ITER(hh,ospv[i],osp,tmp) {
      HASH_DEL(ospv[i], osp);
      free(osp->base);
      kv_spoolwriter_free(osp->sp);
    }
  }
  kv_spoolreader_free(sp);
  kv_set_free(set);
  utarray_free(keys);
  return 0;
}
