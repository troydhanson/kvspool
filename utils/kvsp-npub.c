#include <stdio.h>
#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/prctl.h>
#include <assert.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <nanomsg/nn.h>
#include <nanomsg/pipeline.h>
#include <arpa/inet.h>
#include "utarray.h"
#include "utstring.h"
#include "kvspool.h"
#include "kvsp-bconfig.h"

char *config_file;
UT_string *tmp;
char *remote;    // remote side to which we publish (we do the active open)
int sock,eid;
int verbose;
char *spool;
void *set;
void *sp;

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] -b <cast-config> -d spool <dst-path>\n", prog);
  fprintf(stderr, "  <dst-path> is the nsub peer e.g. tcp://127.0.0.1:1234\n");
  exit(-1);
}

int set_to_binary(void *set) {
  uint32_t l, u, a,b,c,d,e,f, abcd;
  uint16_t s;
  uint8_t g;
  double h;
  utstring_clear(tmp);
  int rc=-1,i=0,*t;
  kv_t *kv, kvdef;
  char **k=NULL,**def;
  while( (k=(char**)utarray_next(output_keys,k))) {
    kv = kv_get(set,*k);
    t = (int*)utarray_eltptr(output_types,i); assert(t);
    def = (char**)utarray_eltptr(output_defaults,i); assert(def);
    if (kv==NULL) { /* no such key */
      kv=&kvdef;
      if (*def) {kv->val=*def; kv->vlen=strlen(*def);} /* default */
      else if (*t == str) {kv->val=NULL; kv->vlen=0;}  /* zero len string */
      else {
        fprintf(stderr,"required key %s not present in spool frame\n", *k);
        goto done;
      }
    }
    switch(*t) {
      case d64: h=atof(kv->val); utstring_bincpy(tmp,&h,sizeof(h)); break;
      case i8:  g=atoi(kv->val); utstring_bincpy(tmp,&g,sizeof(g)); break;
      case i16: s=atoi(kv->val); utstring_bincpy(tmp,&s,sizeof(s)); break;
      case i32: u=atoi(kv->val); utstring_bincpy(tmp,&u,sizeof(u)); break;
      case str: 
        l=kv->vlen; utstring_bincpy(tmp,&l,sizeof(l)); /* length prefix */
        utstring_bincpy(tmp,kv->val,kv->vlen);         /* string itself */
        break;
      case mac: 
        if ((sscanf(kv->val,"%x:%x:%x:%x:%x:%x",&a,&b,&c,&d,&e,&f) != 6) ||
           (a > 255 || b > 255 || c > 255 || d > 255 || e > 255 || f > 255)) {
          fprintf(stderr,"invalid MAC for key %s: %s\n",*k,kv->val);
          goto done;
        }
        g=a; utstring_bincpy(tmp,&g,sizeof(g));
        g=b; utstring_bincpy(tmp,&g,sizeof(g));
        g=c; utstring_bincpy(tmp,&g,sizeof(g));
        g=d; utstring_bincpy(tmp,&g,sizeof(g));
        g=e; utstring_bincpy(tmp,&g,sizeof(g));
        g=f; utstring_bincpy(tmp,&g,sizeof(g));
        break;
      case ipv4: 
        if ((sscanf(kv->val,"%u.%u.%u.%u",&a,&b,&c,&d) != 4) ||
           (a > 255 || b > 255 || c > 255 || d > 255)) {
          fprintf(stderr,"invalid IP for key %s: %s\n",*k,kv->val);
          goto done;
        }
        abcd = (a << 24) | (b << 16) | (c << 8) | d;
        abcd = htonl(abcd);
        utstring_bincpy(tmp,&abcd,sizeof(abcd));
        break;
      default: assert(0); break;
    }
    i++;
  }
  rc = 0;

 done:
  return rc;
}

int main(int argc, char *argv[]) {
  int opt,rc=-1;
  size_t len;
  void *buf;

  set = kv_set_new();
  utarray_new(output_keys, &ut_str_icd);
  utarray_new(output_defaults, &ut_str_icd);
  utarray_new(output_types,&ut_int_icd);
  utstring_new(tmp);

  while ( (opt = getopt(argc, argv, "v+d:b:")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'd': spool=strdup(optarg); break;
      case 'b': config_file=strdup(optarg); break;
      default: usage(argv[0]); break;
    }
  }
  if (optind < argc) remote = argv[optind++];
  if (!remote) usage(argv[0]);
  if (spool == NULL) usage(argv[0]);
  if (parse_config(config_file) < 0) goto done;
  if ( !(sp = kv_spoolreader_new(spool))) goto done;
  rc = -2;

  if ( (sock = nn_socket(AF_SP, NN_PUSH)) < 0) goto done;
  if ( (eid = nn_connect(sock, remote)) < 0) goto done;

  while (kv_spool_read(sp,set,1) > 0) { /* read til interrupted by signal */
    set_to_binary(set);
    buf = utstring_body(tmp);
    len = utstring_len(tmp);
    rc = nn_send(sock, buf, len, 0);
    if (rc == -1) goto done;
  }

  rc = 0;

 done:
  if (rc==-2) fprintf(stderr,"nano: %s %s\n", remote, nn_strerror(errno));
  if (sock) nn_shutdown(sock,eid);
  if (sp) kv_spoolreader_free(sp);
  kv_set_free(set);
  utarray_free(output_keys);
  utarray_free(output_defaults);
  utarray_free(output_types);
  utstring_free(tmp);

  return 0;
}

