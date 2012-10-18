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
#include <zmq.h>
#include <arpa/inet.h>
#include "utarray.h"
#include "utstring.h"
#include "kvspool.h"

#if ZMQ_VERSION_MAJOR == 2
#define zmq_sendmsg zmq_send
#define zmq_recvmsg zmq_recv
#define zmq_hwm_t uint64_t
#define ZMQ_SNDHWM ZMQ_HWM
#else
#define zmq_hwm_t int
#endif

const zmq_hwm_t hwm = 10000; /* high water mark: max messages pub will buffer */
char *pub_transport;         /* clients connect to us on this transport */
void *pub_socket;
void *pub_context;
int verbose;
char *spool;
int push_mode;
UT_string *tmp;

UT_array /* of string */ *output_keys;
UT_array /* of int */    *output_types;

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] [-s] -d spool <path>\n", prog);
  fprintf(stderr, "  -s runs in push-pull mode instead of lossy pub/sub\n");
  fprintf(stderr, "  <path> is a 0mq path e.g. tcp://127.0.0.1:1234\n");
  exit(-1);
}
#define TYPES x(i16) x(i32) x(ipv4) x(str)
#define x(t) #t,
char *supported_types_str[] = { TYPES };
#undef x
#define x(t) t,
enum supported_types { TYPES };
#undef x
#define adim(a) (sizeof(a)/sizeof(*a))
int parse_config(char *config_file) {
  char line[100];
  FILE *file;
  int rc=-1;
  int type,t;
  char *sp,*nl;
  if ( (file = fopen(config_file,"r")) == NULL) {
    fprintf(stderr,"can't open %s: %s\n", config_file, strerror(errno));
    goto done;
  }
  while (fgets(line,sizeof(line),file) != NULL) {
    sp = strchr(line,' '); if (!sp) continue;
    nl = strchr(line,'\n'); if (nl) *nl='\0';
    for(t=0; t<adim(supported_types_str); t++) {
      if(!strncmp(supported_types_str[t],line,sp-line)) break;
    }
    if (t >= adim(supported_types_str)){
      fprintf(stderr,"unknown type %s\n",line); 
      goto done;
    }
    char *id = strdup(sp+1);
    utarray_push_back(output_types,&t);
    utarray_push_back(output_keys,&id);
  }
  rc = 0;
 done:
  if (file) fclose(file);
  return rc;
}

int set_to_binary(void *set, zmq_msg_t *part) {
  uint32_t l, u, a,b,c,d, abcd;
  uint16_t s;
  utstring_clear(tmp);
  int rc=-1,i=0,*t;
  kv_t *kv;
  char **k=NULL;
  while( (k=(char**)utarray_next(output_keys,k))) {
    kv = kv_get(set,*k);
    t = (int*)utarray_eltptr(output_types,i); assert(t && *t);
    if (kv==NULL) {  /* only string-valued types are considered optional  */
                     /* since we can send them as zero length strings */
      if (*t != str) {
        fprintf(stderr,"required key %s not present in spool frame\n", *k);
        goto done;
      }
      l = 0; utstring_bincpy(tmp,&l,sizeof(l)); /* pack zero len string */
    } else {
      switch(*t) {
        case i16: s=atoi(kv->val); utstring_bincpy(tmp,&s,sizeof(s)); break;
        case i32: u=atoi(kv->val); utstring_bincpy(tmp,&u,sizeof(u)); break;
        case str: 
          l=kv->vlen; utstring_bincpy(tmp,&l,sizeof(l)); /* length prefix */
          utstring_bincpy(tmp,kv->val,kv->vlen);         /* string itself */
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
    }
    i++;
  }
  size_t len = utstring_len(tmp);
  rc = zmq_msg_init_size(part,len); if (rc) goto done;
  memcpy(zmq_msg_data(part), utstring_body(tmp), len);
  rc = 0;

 done:
  return rc;
}

int main(int argc, char *argv[]) {
  void *sp=NULL;
  void *set=NULL;
  int opt,rc=-1;
  char *config_file;
  set = kv_set_new();
  utarray_new(output_keys, &ut_str_icd);
  utarray_new(output_types,&ut_int_icd);
  utstring_new(tmp);

  while ( (opt = getopt(argc, argv, "v+d:b:s")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 's': push_mode++; break;
      case 'd': spool=optarg; break;
      case 'b': config_file=strdup(optarg); break;
      default: usage(argv[0]); break;
    }
  }
  if (optind < argc) pub_transport = argv[optind++];
  if (!pub_transport) usage(argv[0]);
  if (spool == NULL) usage(argv[0]);
  if (parse_config(config_file) < 0) goto done;

  if ( !(pub_context = zmq_init(1))) goto done;
  if ( !(pub_socket = zmq_socket(pub_context, push_mode?ZMQ_PUSH:ZMQ_PUB))) goto done;
  if (zmq_setsockopt(pub_socket, ZMQ_SNDHWM, &hwm, sizeof(hwm))) goto done;
  if (zmq_bind(pub_socket, pub_transport) == -1) goto done;

  sp = kv_spoolreader_new(spool);
  if (!sp) goto done;

  while (kv_spool_read(sp,set,1) > 0) { /* read til interrupted by signal */
    zmq_msg_t part;
    if (set_to_binary(set,&part) < 0) goto done;
    rc = zmq_sendmsg(pub_socket, &part, 0);
    zmq_msg_close(&part);
    if (rc == -1) goto done;
  }

  rc = 0;

 done:
  if (rc) fprintf(stderr,"zmq: %s %s\n", pub_transport, zmq_strerror(errno));
  if (pub_socket) zmq_close(pub_socket);
  if (pub_context) zmq_term(pub_context);
  if (sp) kv_spoolreader_free(sp);
  kv_set_free(set);
  utarray_free(output_keys);
  utarray_free(output_types);
  utstring_free(tmp);

  return 0;
}

