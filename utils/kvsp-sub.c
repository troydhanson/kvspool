#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <time.h>
#include <zmq.h>

#include "kvspool_internal.h"
#include "utstring.h"
#include "uthash.h"

void *sp;

int verbose;
int pull_mode;
char *dir;
char *pub;

void *context;
void *socket;


void usage(char *exe) {
  fprintf(stderr,"usage: %s [-v] [-s] -d <dir> <pub>\n", exe);
  fprintf(stderr,"       -s runs in push-pull mode instead of lossy pub-sub\n");
  exit(-1);
}

#if ZMQ_VERSION_MAJOR == 2
#define zmq_sendmsg zmq_send
#define zmq_recvmsg zmq_recv
#define zmq_rcvmore_t int64_t
#else
#define zmq_rcvmore_t int
#endif
int main(int argc, char *argv[]) {

  zmq_rcvmore_t more; size_t more_sz = sizeof(more);
  char *exe = argv[0], *filter = "";
  int part_num,opt,rc=-1;
  void *msg_data, *sp;
  size_t msg_len;
  zmq_msg_t part;

  while ( (opt = getopt(argc, argv, "sd:v+")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 's': pull_mode++; break;
      case 'd': dir=strdup(optarg); break;
      default: usage(exe); break;
    }
  }
  if (optind < argc) pub=argv[optind++];
  if (!dir) usage(exe);
  if (!pub) usage(exe);
  sp = kv_spoolwriter_new(dir);
  if (!sp) usage(exe);

  if ( !(context = zmq_init(1))) goto done;
  if ( !(socket = zmq_socket(context, pull_mode?ZMQ_PULL:ZMQ_SUB))) goto done;
  if (zmq_connect(socket, pub)) goto done;
  if (!pull_mode) {
    if (zmq_setsockopt(socket, ZMQ_SUBSCRIBE, filter, strlen(filter))) goto done;
  }

  while(1) {

    /* receive a multi-part message */
    part_num=1;
    do {
      if ( (rc= zmq_msg_init(&part))) goto done;
      if ( ((rc= zmq_recvmsg(socket, &part, 0)) == -1) || 
           ((rc= zmq_getsockopt(socket, ZMQ_RCVMORE, &more,&more_sz)) != 0)) {
        zmq_msg_close(&part);
        goto done;
      }

      msg_data = zmq_msg_data(&part);
      msg_len = zmq_msg_size(&part);
       
      switch(part_num) {  /* part 1 has serialized frame */
        case 1: if (kv_write_raw_frame(sp,msg_data,msg_len)) goto done; break;
        default: assert(0); 
      }

      zmq_msg_close(&part);
      part_num++;
    } while(more);
  }
  rc = 0; /* not reached TODO under clean shutdown on signal */


 done:
  if (rc) fprintf(stderr,"%s: %s\n", exe, zmq_strerror(errno));
  if(socket) zmq_close(socket);
  if(context) zmq_term(context);
  kv_spoolwriter_free(sp);
  return rc;
}

