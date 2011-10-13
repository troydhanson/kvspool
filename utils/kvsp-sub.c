#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <time.h>
#include <zmq.h>

#include "kvspool_internal.h"
#include "utstring.h"
#include "uthash.h"

typedef struct {  /* helper that maps base --> spool handle */
  char *base;
  void *sp;
  UT_hash_handle hh;
} sp_t;

int verbose=0;
char *dir;
char *file;
char *pub;

void *context;
void *socket;
sp_t *spools;


void usage(char *exe) {
  fprintf(stderr,"usage: %s [-v] [-f <file> | -d <dir>] <pub>\n", exe);
  exit(-1);
}

/* finds the spool handle for a given base, creating if necessary */
void *get_spool(char *_base, size_t len) {
  sp_t *spt;

  HASH_FIND(hh,spools,_base,len,spt);
  if (spt) return spt->sp;

  spt = malloc(sizeof(*spt)); assert(spt); memset(spt,0,sizeof(*spt));
  spt->base = strndup(_base,len);
  spt->sp = kv_spoolwriter_new(dir,spt->base);
  if (!spt->sp) { free(spt->base); free(spt); return NULL; }
  HASH_ADD_KEYPTR(hh,spools,spt->base,len,spt);
  return spt->sp;
}

void close_spools(void) {
  sp_t *spt, *tmp;
  HASH_ITER(hh,spools,spt,tmp) {
    HASH_DEL(spools,spt);
    free(spt->base);
    kv_spoolwriter_free(spt->sp);
  }
}

int main(int argc, char *argv[]) {

  int64_t more; size_t more_sz = sizeof(more);
  char *exe = argv[0], *filter = "";
  int part_num,opt,rc=-1;
  void *msg_data, *sp;
  size_t msg_len;
  zmq_msg_t part;

  while ( (opt = getopt(argc, argv, "fd:v+")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'f': file=strdup(optarg); break;
      case 'd': dir=strdup(optarg); break;
      default: usage(exe); break;
    }
  }
  if (optind < argc) pub=argv[optind++];
  if (!dir && !file) usage(exe);
  if (!pub) usage(exe);

  if ( !(context = zmq_init(1))) goto done;
  if ( !(socket = zmq_socket(context, ZMQ_SUB))) goto done;
  if (zmq_connect(socket, pub)) goto done;
  if (zmq_setsockopt(socket, ZMQ_SUBSCRIBE, filter, strlen(filter))) goto done;

  while(1) {

    /* receive a multi-part message */
    part_num=1;
    do {
      if ( (rc= zmq_msg_init(&part))) goto done;
      if ( ((rc= zmq_recv(socket, &part, 0)) != 0) || 
           ((rc= zmq_getsockopt(socket, ZMQ_RCVMORE, &more,&more_sz)) != 0)) {
        zmq_msg_close(&part);
        goto done;
      }

      msg_data = zmq_msg_data(&part);
      msg_len = zmq_msg_size(&part);
       
      switch(part_num) {  /* part 1 has the base; part 2 has serialized frame */
        case 1: sp=get_spool(msg_data,msg_len); break;
        case 2: if (kv_write_raw_frame(sp,msg_data,msg_len)) goto done; break;
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
  close_spools();
  return rc;
}

