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

char *remotes_file;
char *config_file;
UT_string *tmp;
int sock,eid;
int verbose;
char *spool;
void *set;
void *sp;

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] -b <cast-config> -d spool [-f <remotes-file>] [<remote> ...]\n", prog);
  fprintf(stderr, "  <remote> is the nsub peer e.g. tcp://127.0.0.1:1234\n");
  exit(-1);
}

int read_lines(char *file, UT_array *lines) {
  char line[200];
  int rc = -1;
  char *c;
  FILE *f = fopen(file,"r");
  if (f==NULL) {
    fprintf(stderr,"fopen %s: %s\n", file, strerror(errno));
    goto done;
  }
  while (fgets(line,sizeof(line),f) != NULL) {
    for(c=line; (c < line+sizeof(line)) && (*c != '\0'); c++) {
     if (*c == '\n') *c='\0';
     if (*c == ' ') *c='\0';
    }
    c = line;
    if (strlen(c) == 0) continue;
    utarray_push_back(lines,&c);
  }
  rc  = 0;

 done:
  if (f) fclose(f);
  return rc;
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
      case str8: 
        g=kv->vlen; utstring_bincpy(tmp,&g,sizeof(g)); /* length prefix */
        utstring_bincpy(tmp,kv->val,g);                /* string itself */
        break;
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
      case ipv46: 
        if ((sscanf(kv->val,"%u.%u.%u.%u",&a,&b,&c,&d) != 4) ||
           (a > 255 || b > 255 || c > 255 || d > 255)) {
          fprintf(stderr,"invalid IP for key %s: %s\n",*k,kv->val);
          // FIXME try v6 interp
          goto done;
        }
        abcd = (a << 24) | (b << 16) | (c << 8) | d;
        abcd = htonl(abcd);
        g=4; utstring_bincpy(tmp,&g,sizeof(g));
        utstring_bincpy(tmp,&abcd,sizeof(abcd));
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
  UT_array *endpoints;
  char **endpoint;

  set = kv_set_new();
  utarray_new(output_keys, &ut_str_icd);
  utarray_new(output_defaults, &ut_str_icd);
  utarray_new(output_types,&ut_int_icd);
  utstring_new(tmp);
  utarray_new(endpoints,&ut_str_icd);

  while ( (opt = getopt(argc, argv, "v+d:b:f:h")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'd': spool=strdup(optarg); break;
      case 'b': config_file=strdup(optarg); break;
      case 'f': remotes_file=strdup(optarg); break;
      case 'h': default: usage(argv[0]); break;
    }
  }
  if (remotes_file) if (read_lines(remotes_file,endpoints)) goto done;
  while (optind < argc) utarray_push_back(endpoints,&argv[optind++]);
  if (utarray_len(endpoints) == 0) usage(argv[0]);
  if (spool == NULL) usage(argv[0]);
  if (parse_config(config_file) < 0) goto done;
  if ( !(sp = kv_spoolreader_new(spool))) goto done;
  rc = -2;

  if ( (sock = nn_socket(AF_SP, NN_PUSH)) < 0) goto done;
  endpoint=NULL;
  while ( (endpoint=(char**)utarray_next(endpoints,endpoint))) {
    if (verbose) fprintf(stderr,"connecting to %s\n", *endpoint);
    if ( (eid = nn_connect(sock, *endpoint)) < 0) goto done;
  }

  while (kv_spool_read(sp,set,1) > 0) { /* read til interrupted by signal */
    set_to_binary(set);
    buf = utstring_body(tmp);
    len = utstring_len(tmp);
    rc = nn_send(sock, buf, len, 0);
    if (rc == -1) goto done;
  }

  rc = 0;

 done:
  if (rc==-2) fprintf(stderr,"nano: %s\n", nn_strerror(errno));
  if (sock) nn_close(sock);
  if (sp) kv_spoolreader_free(sp);
  kv_set_free(set);
  utarray_free(output_keys);
  utarray_free(output_defaults);
  utarray_free(output_types);
  utstring_free(tmp);
  utarray_free(endpoints);

  return 0;
}

