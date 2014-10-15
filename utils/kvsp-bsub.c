#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <time.h>
#include <zmq.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>


#include "kvspool_internal.h"
#include "utstring.h"
#include "uthash.h"
#include "kvsp-bconfig.h"

void *sp;
int verbose;
int pull_mode;
char *dir;
char *remotes_file;
void *context;
void *zsocket;
UT_string *val;


void usage(char *exe) {
  fprintf(stderr,"usage: %s [-v] -b <config> [-s] -d <dir> [-f file | <pub> ...]\n", exe);
  fprintf(stderr,"       -s runs in push-pull mode instead of lossy pub-sub\n");
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

#if ZMQ_VERSION_MAJOR == 2
#define zmq_sendmsg zmq_send
#define zmq_recvmsg zmq_recv
#define zmq_rcvmore_t int64_t
#else
#define zmq_rcvmore_t int
#endif

int get(void **msg_data,size_t *msg_len,void *dst,size_t len) {
  if (*msg_len < len) {
    fprintf(stderr,"received message shorter than expected\n"); 
    return -1;
  }
  memcpy(dst,*msg_data,len);
  *(char**)msg_data += len;
  *msg_len -= len;
  return 0;
}

int binary_to_frame(void *sp, void *set, void *msg_data, size_t msg_len) {
  int rc=-1,i=0,*t;
  const char *key;
  struct in_addr ia;

  uint32_t l, u, a,b,c,d, abcd;
  uint16_t s;
  uint8_t g;
  double h;

  kv_set_clear(set);
  char **k = NULL;
  while ( (k=(char**)utarray_next(output_keys,k))) {
    t = (int*)utarray_eltptr(output_types,i); assert(t);
    // type is *t and key is *k
    utstring_clear(val);
    switch(*t) {
      case d64: if (get(&msg_data,&msg_len,&h,sizeof(h))<0) goto done; utstring_printf(val,"%f",h); break;
      case i8:  if (get(&msg_data,&msg_len,&g,sizeof(g))<0) goto done; utstring_printf(val,"%d",(int)g); break;
      case i16: if (get(&msg_data,&msg_len,&s,sizeof(s))<0) goto done; utstring_printf(val,"%d",(int)s); break;
      case i32: if (get(&msg_data,&msg_len,&u,sizeof(u))<0) goto done; utstring_printf(val,"%d",u); break;
      case str: 
        if (get(&msg_data,&msg_len,&l,sizeof(l)) < 0) goto done;
        utstring_reserve(val,l);
        if (get(&msg_data,&msg_len,utstring_body(val),l) < 0) goto done;
        val->i += l;
        break;
      case ipv4: 
        if (get(&msg_data,&msg_len,&abcd,sizeof(abcd)) < 0) goto done;
        ia.s_addr = abcd;
        utstring_printf(val,"%s", inet_ntoa(ia));
        break;
      default: assert(0); break;
    }
    i++;
    key = *k;
    kv_add(set, key, strlen(key), utstring_body(val), utstring_len(val));
  }
  kv_spool_write(sp, set);

  rc = 0;

 done:
  return rc;
}

int main(int argc, char *argv[]) {

  zmq_rcvmore_t more; size_t more_sz = sizeof(more);
  char *exe = argv[0], *filter = "";
  int part_num,opt,rc=-1;
  void *msg_data, *sp, *set=NULL;
  char *config_file, **endpoint;
  UT_array *endpoints;
  size_t msg_len;
  zmq_msg_t part;
  utstring_new(val);
  utarray_new(output_keys, &ut_str_icd);
  utarray_new(output_defaults, &ut_str_icd);
  utarray_new(output_types,&ut_int_icd);
  utarray_new(endpoints,&ut_str_icd);

  while ( (opt = getopt(argc, argv, "sd:b:f:v+")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 's': pull_mode++; break;
      case 'd': dir=strdup(optarg); break;
      case 'b': config_file=strdup(optarg); break;
      case 'f': remotes_file=strdup(optarg); break;
      default: usage(exe); break;
    }
  }
  if (!dir) usage(exe);
  if (parse_config(config_file) < 0) goto done;

  sp = kv_spoolwriter_new(dir);
  if (!sp) usage(exe);
  set = kv_set_new();

  /* connect socket to each publisher. yes, zeromq lets you connect n times */
  if ( !(context = zmq_init(1))) goto done;
  if ( !(zsocket = zmq_socket(context, pull_mode?ZMQ_PULL:ZMQ_SUB))) goto done;
  if (remotes_file) if (read_lines(remotes_file,endpoints)) goto done;
  while (optind < argc) utarray_push_back(endpoints,&argv[optind++]);
  endpoint=NULL;
  if (utarray_len(endpoints) == 0) usage(argv[0]);
  while ( (endpoint=(char**)utarray_next(endpoints,endpoint))) {
    if (verbose) fprintf(stderr,"connecting to %s\n", *endpoint);
    if (zmq_connect(zsocket, *endpoint)) goto done;
  }
  if (!pull_mode) {
    if (zmq_setsockopt(zsocket, ZMQ_SUBSCRIBE, filter, strlen(filter))) goto done;
  }

  while(1) {

    /* receive a multi-part message */
    part_num=1;
    do {
      if ( (rc= zmq_msg_init(&part))) goto done;
      if ( ((rc= zmq_recvmsg(zsocket, &part, 0)) == -1) || 
           ((rc= zmq_getsockopt(zsocket, ZMQ_RCVMORE, &more,&more_sz)) != 0)) {
        zmq_msg_close(&part);
        goto done;
      }

      msg_data = zmq_msg_data(&part);
      msg_len = zmq_msg_size(&part);
       
      switch(part_num) {  /* part 1 has serialized frame */
        case 1: if (binary_to_frame(sp,set,msg_data,msg_len)) goto done; break;
        default: assert(0); 
      }

      zmq_msg_close(&part);
      part_num++;
    } while(more);
  }
  rc = 0; /* not reached TODO under clean shutdown on signal */


 done:
  if (rc) fprintf(stderr,"%s: %s\n", exe, zmq_strerror(errno));
  if(zsocket) zmq_close(zsocket);
  if(context) zmq_term(context);
  kv_spoolwriter_free(sp);
  if (set) kv_set_free(set);
  utarray_free(output_keys);
  utarray_free(output_defaults);
  utarray_free(output_types);
  utarray_free(endpoints);
  utstring_free(val);
  return rc;
}

