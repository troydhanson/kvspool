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
#include <jansson.h>
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

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] [-s] -d spool <path>\n", prog);
  fprintf(stderr, "  -s runs in push-pull mode instead of lossy pub/sub\n");
  fprintf(stderr, "  <path> is a 0mq path e.g. tcp://127.0.0.1:1234\n");
  exit(-1);
}

int main(int argc, char *argv[]) {
  void *sp=NULL;
  void *set=NULL;
  int opt,rc=-1;
  json_t *o = NULL;

  while ( (opt = getopt(argc, argv, "v+d:s")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 's': push_mode++; break;
      case 'd': spool=strdup(optarg); break;
      default: usage(argv[0]); break;
    }
  }
  if (optind < argc) pub_transport = argv[optind++];
  if (!pub_transport) usage(argv[0]);
  if (spool == NULL) usage(argv[0]);

  if ( !(pub_context = zmq_init(1))) goto done;
  if ( !(pub_socket = zmq_socket(pub_context, push_mode?ZMQ_PUSH:ZMQ_PUB))) goto done;
  if (zmq_setsockopt(pub_socket, ZMQ_SNDHWM, &hwm, sizeof(hwm))) goto done;
  if (zmq_bind(pub_socket, pub_transport) == -1) goto done;

  set = kv_set_new();
  sp = kv_spoolreader_new(spool);
  if (!sp) goto done;
  o = json_object();

  while (kv_spool_read(sp,set,1) > 0) { /* read til interrupted by signal */
    zmq_msg_t part;
    json_object_clear(o);
    kv_t *kv = NULL;
    while (kv = kv_next(set,kv)) {
      json_t *jval = json_string(kv->val);
      json_object_set_new(o, kv->key, jval); 
    }
    if (verbose) json_dumpf(o, stderr, JSON_INDENT(1));
    char *json  = json_dumps(o, JSON_INDENT(1));
    size_t len = strlen(json);
    rc = zmq_msg_init_size(&part,len); if (rc) goto done;
    memcpy(zmq_msg_data(&part), json, len);
    free(json);
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
  if(o) json_decref(o);

  return 0;
}

