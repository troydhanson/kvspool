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
#include "kvsp-bconfig.h"

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

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] -b <config> [-s] -d spool <path>\n", prog);
  fprintf(stderr, "  -s runs in push-pull mode instead of lossy pub/sub\n");
  fprintf(stderr, "  <path> is a 0mq path e.g. tcp://127.0.0.1:1234\n");
  exit(-1);
}

int main(int argc, char *argv[]) {
  void *sp=NULL;
  void *set=NULL;
  int opt,rc=-1;
  char *config_file, *bin;
  size_t len;
  set = kv_set_new();
  utarray_new(output_keys, &ut_str_icd);
  utarray_new(output_defaults, &ut_str_icd);
  utarray_new(output_types,&ut_int_icd);
  utstring_new(tmp);

  while ( (opt = getopt(argc, argv, "v+d:b:s")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 's': push_mode++; break;
      case 'd': spool=strdup(optarg); break;
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

    if (set_to_binary(set,tmp) < 0) goto done;
    len = utstring_len(tmp);
    bin = utstring_body(tmp);

    /* skip length preamble */
    if (len <= sizeof(uint32_t)) goto done;
    len -= sizeof(uint32_t);
    bin += sizeof(uint32_t);

    zmq_msg_t part;
    rc = zmq_msg_init_size(&part,len);
    if (rc) goto done;
    memcpy(zmq_msg_data(&part), bin, len);
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
  utarray_free(output_defaults);
  utarray_free(output_types);
  utstring_free(tmp);

  return 0;
}

