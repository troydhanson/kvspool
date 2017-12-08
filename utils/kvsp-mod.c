#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "kvspool.h"
#include "utarray.h"
#include "utstring.h"

int verbose=0;
char *dir = NULL;
char *conf = NULL;
char *odir = ".";
UT_array *keys;

void *osp = NULL;

void usage(char *exe) {
  fprintf(stderr,"usage: %s [-v]  -k <key> "
                 "[-k <key>...] [-o outdir] <spooldir>\n", exe);
  exit(-1);
}

int main(int argc, char *argv[]) {

  kv_t *kv;
  int opt,rc;
  char *exe = argv[0];
  utarray_new(keys,&ut_str_icd);

  while ( (opt = getopt(argc, argv, "v+k:c:n:o:")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'c': conf=strdup(optarg); break;
      case 'o': odir=strdup(optarg); break;
      case 'k': utarray_push_back(keys,&optarg); break;
      default: usage(exe); break;
    }
  }
  if (optind < argc) dir=argv[optind++];
  else usage(exe);

  osp = kv_spoolwriter_new(odir);
  if (!osp) exit(-1);

  void *sp = kv_spoolreader_new(dir);
  if (!sp) exit(-1);

  void *set = kv_set_new();
  while ( (rc=kv_spool_read(sp,set,1)) > 0) {
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
    if (kv_spool_write(osp, set) != 0) {printf("output error\n"); break; }
  }

  /* clean up */
  kv_spoolwriter_free(osp);
  kv_spoolreader_free(sp);
  kv_set_free(set);
  utarray_free(keys);
  return 0;
}
