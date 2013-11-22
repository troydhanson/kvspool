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
#include <arpa/inet.h>
#include "utarray.h"
#include "utstring.h"
#include "kvspool.h"
#include "kvsp-bconfig.h"

int verbose;
int port;
int fd;  // listening socket 
int fa;  // socket to client
char *spool;
UT_string *buf;

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] -b <config> -d spool -p <port>\n", prog);
  exit(-1);
}

int setup_listener() {
  int rc=-1;

  /**********************************************************
   * create an IPv4/TCP socket, not yet bound to any address
   *********************************************************/
  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    fprintf(stderr,"socket: %s\n", strerror(errno));
    goto done;
  }

  int one=1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  /**********************************************************
   * internet socket address structure: our address and port
   *********************************************************/
  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = htonl(INADDR_ANY);
  sin.sin_port = htons(port);

  /**********************************************************
   * bind socket to address and port we'd like to receive on
   *********************************************************/
  if (bind(fd, (struct sockaddr*)&sin, sizeof(sin)) == -1) {
    fprintf(stderr,"bind: %s\n", strerror(errno));
    goto done;
  }

  /**********************************************************
   * put socket into listening state 
   *********************************************************/
  if (listen(fd,1) == -1) {
    fprintf(stderr,"listen: %s\n", strerror(errno));
    goto done;
  }
  rc = 0;

 done:
  return rc;
}

int accept_connection() {
  int rc=-1;

  struct sockaddr_in cin;
  socklen_t cin_sz = sizeof(cin);
  fa = accept(fd, (struct sockaddr*)&cin, &cin_sz);
  if (fa == -1) {
    fprintf(stderr,"accept: %s\n", strerror(errno));
    goto done;
  }
  if (verbose && (sizeof(cin)==cin_sz)) 
    fprintf(stderr, "connection from %s:%d\n", 
    inet_ntoa(cin.sin_addr), (int)ntohs(cin.sin_port));

  rc = 0;

 done:
  return rc;
}

int set_to_binary(void *set, UT_string *bin) {
  uint32_t l, u, a,b,c,d, abcd;
  uint16_t s;
  uint8_t g;
  double h;
  utstring_clear(bin);
  l=0; utstring_bincpy(bin,&l,sizeof(l)); // placeholder for size prefix
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
      case d64: h=atof(kv->val); utstring_bincpy(bin,&h,sizeof(h)); break;
      case i8:  g=atoi(kv->val); utstring_bincpy(bin,&g,sizeof(g)); break;
      case i16: s=atoi(kv->val); utstring_bincpy(bin,&s,sizeof(s)); break;
      case i32: u=atoi(kv->val); utstring_bincpy(bin,&u,sizeof(u)); break;
      case str: 
        l=kv->vlen; utstring_bincpy(bin,&l,sizeof(l)); /* length prefix */
        utstring_bincpy(bin,kv->val,kv->vlen);         /* string itself */
        break;
      case ipv4: 
        if ((sscanf(kv->val,"%u.%u.%u.%u",&a,&b,&c,&d) != 4) ||
           (a > 255 || b > 255 || c > 255 || d > 255)) {
          fprintf(stderr,"invalid IP for key %s: %s\n",*k,kv->val);
          goto done;
        }
        abcd = (a << 24) | (b << 16) | (c << 8) | d;
        abcd = htonl(abcd);
        utstring_bincpy(bin,&abcd,sizeof(abcd));
        break;
      default: assert(0); break;
    }
    i++;
  }
  uint32_t len = utstring_len(bin); len -= sizeof(len); // length does not include itself
  char *length_prefix = utstring_body(bin);
  memcpy(length_prefix, &len, sizeof(len));

  rc = 0;

 done:
  return rc;
}

int main(int argc, char *argv[]) {
  void *sp=NULL;
  void *set=NULL;
  int c, opt,rc=-1;
  size_t sz; void *b;
  char *config_file;
  set = kv_set_new();
  utarray_new(output_keys, &ut_str_icd);
  utarray_new(output_defaults, &ut_str_icd);
  utarray_new(output_types,&ut_int_icd);
  utstring_new(buf);

  signal(SIGPIPE,SIG_IGN);

  while ( (opt = getopt(argc, argv, "v+d:b:p:")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'd': spool=optarg; break;
      case 'b': config_file=strdup(optarg); break;
      case 'p': port=atoi(optarg); break;
      default: usage(argv[0]); break;
    }
  }
  if (spool == NULL) usage(argv[0]);
  if (port == 0) usage(argv[0]);
  if (parse_config(config_file) < 0) goto done;

  sp = kv_spoolreader_new(spool);
  if (!sp) goto done;

  if (setup_listener()) goto done;
  if (accept_connection()) goto done;
  // TODO multiple clients
  // TODO respond to remote close even while waiting on spool read

  while (kv_spool_read(sp,set,1) > 0) { /* read til interrupted by signal */
    if (set_to_binary(set,buf) < 0) goto done;
    b = utstring_body(buf);
    sz = utstring_len(buf);
    do {
      c = write(fa, b, sz); 
      if (c <= 0) {
        fprintf(stderr,"write error: %s\n",c?strerror(errno):"remote close");
        goto done;
      }
      b += c;
      sz -= c;
    } while(sz);
  }

  close(fa);


  rc = 0;

 done:
  if (sp) kv_spoolreader_free(sp);
  kv_set_free(set);
  utarray_free(output_keys);
  utarray_free(output_defaults);
  utarray_free(output_types);
  utstring_free(buf);

  return 0;
}

