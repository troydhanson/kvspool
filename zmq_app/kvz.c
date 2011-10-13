#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#include <zmq.h>
#include "kvspool.h"
#include "zcontrol.h"
#include "utarray.h"
#include "kvz.h"

struct _CF CF = {
  .zcontrol_endpoint = ZCON_DEF_ENDPOINT,
};

ts_mm hits_1h_mm = {.sz=sizeof(int)};

extern cp_cmd_t cmds[];

static void s_signal_handler (int signal_value) {
  CF.request_exit = 1;
}

static void s_catch_signals (void) {
  struct sigaction action;
  action.sa_handler = s_signal_handler;
  action.sa_flags = 0;
  sigemptyset (&action.sa_mask);
  sigaction (SIGINT, &action, NULL);
  sigaction (SIGTERM, &action, NULL);
}

void show_version(char *prog) {
  fprintf(stderr, "%s 0.1\n", prog);
  int major, minor, patch;
  zmq_version (&major, &minor, &patch);
  fprintf(stderr,"Current 0MQ version is %d.%d.%d\n", major, minor, patch);
  exit(-1);
}

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] [-s spool] [-x] [-V]\n", prog);
  exit(-1);
}

// this function runs in a separate thread that pushes to main thread
static void* spr(void *data) {
  void *sp=NULL;
  int rc=-1, block=!CF.exit_when_spool_consumed;

  if ( (sp = kv_spoolreader_new(CF.spool,NULL)) == NULL) {
    fprintf(stderr, "failed to open %s\n", CF.spool);
    goto abnormal_exit;
  }

  zmq_msg_t msg;
  void *set;
  while(CF.request_exit==0) {
    set = kv_set_new();
    if (kv_spool_read(sp, set, block) != 1) break;
    if (zmq_msg_init_size(&msg, sizeof(void*))) break;
    memcpy(zmq_msg_data(&msg), &set, sizeof(void*));
    if (zmq_send(CF.spr_push_socket, &msg, 0)) break;
    if (zmq_msg_close(&msg)) break;
    CF.frames_read++;
  } 

 abnormal_exit:
  if (sp) kv_spoolreader_free(sp);
  fprintf(stderr, "spool reader exiting\n");
  CF.request_exit=1;
  // kick the main loop to fall out of msg-recv and honor exit request */
  zmq_msg_init_size(&msg, 0); 
  zmq_send(CF.spr_push_socket, &msg, 0); 
  zmq_msg_close(&msg);
  return NULL;
}

int setup_zmq() {
  int zero=0; uint64_t one=1;
  if  (( (CF.zmq_context = zmq_init(1)) == NULL)                              || 
       ( (CF.spr_push_socket = zmq_socket(CF.zmq_context, ZMQ_PUSH)) == NULL) ||
       ( (CF.spr_pull_socket = zmq_socket(CF.zmq_context, ZMQ_PULL)) == NULL) ||
       ( (CF.zcontrol_socket  = zmq_socket(CF.zmq_context, ZMQ_REP )) == NULL) ||
       ( zmq_setsockopt(CF.spr_push_socket, ZMQ_LINGER, &zero, sizeof(zero))) ||
       ( zmq_setsockopt(CF.spr_pull_socket, ZMQ_LINGER, &zero, sizeof(zero))) ||
       ( zmq_setsockopt(CF.spr_push_socket, ZMQ_HWM, &one, sizeof(one)))      ||
       ( zmq_setsockopt(CF.zcontrol_socket , ZMQ_LINGER, &zero, sizeof(zero))) ||
       ( zmq_bind(CF.zcontrol_socket, CF.zcontrol_endpoint ) != 0)                ||
       ( zmq_bind(   CF.spr_push_socket, "inproc://spr") != 0)                ||
       ( zmq_connect(CF.spr_pull_socket, "inproc://spr") != 0)) {
    fprintf(stderr,"zeromq setup failed: %s\n", zmq_strerror(errno));
    return -1;
  }
  return 0;
}
 
void msg_loop() {

  zmq_pollitem_t items [] = {
    { CF.spr_pull_socket, 0, ZMQ_POLLIN, 0 },
    { CF.zcontrol_socket, 0, ZMQ_POLLIN, 0 },
  };

  while (CF.request_exit==0) {
    zmq_msg_t msg;
    zmq_poll (items, adim(items), -1);

    if (items[0].revents & ZMQ_POLLIN) { // spool reader socket
      zmq_msg_init (&msg);
      if (zmq_recv(CF.spr_pull_socket, &msg, 0)) {
        fprintf(stderr,"zmq_recv: %s\n", zmq_strerror(errno)); break;
      }
      void **_set = zmq_msg_data(&msg); void *set = *_set;
      if (zmq_msg_size(&msg) == sizeof(void*)) {
        /* set is the kv-set */
        /* fill in application logic here */
        ts_add(CF.frames_1h, time(NULL), NULL);
        kv_set_free(set);
      }
      zmq_msg_close(&msg);
      CF.frames_processed++;
    }

    if (items[1].revents & ZMQ_POLLIN) { // control socket
      cp_exec(CF.zcontrol, CF.zcontrol_socket);
      CF.controls++;
    }
  }
}

int main(int argc, char *argv[]) {
  int opt,keepgoing=1;
  pthread_t th;
  void *res;

  while ( (opt = getopt(argc, argv, "v+xs:Vq:")) != -1) {
    switch (opt) {
      case 'v': CF.verbose++; break;
      case 'V': show_version(argv[0]); break;
      case 's': CF.spool=strdup(optarg); break;
      case 'x': CF.exit_when_spool_consumed=1; break;
      case 'q': CF.zcontrol_endpoint=strdup(optarg); break;
      default: usage(argv[0]); break;
    }
  }

  if (!CF.spool) usage(argv[0]);
  if (setup_zmq() == -1) goto program_exit;

  CF.frames_1h = ts_new(60,60,&hits_1h_mm);
  CF.zcontrol = cp_init(cmds,NULL);
  s_catch_signals();
  pthread_create(&CF.spr_thread,NULL,spr,NULL);
  msg_loop();

 program_exit:
  fprintf(stderr,"frames read:      %u\n", CF.frames_read);
  fprintf(stderr,"frames processed: %u\n", CF.frames_processed);
  fprintf(stderr,"control commands: %u\n", CF.controls);
  if (CF.spr_push_socket) zmq_close(CF.spr_push_socket);
  if (CF.spr_pull_socket) zmq_close(CF.spr_pull_socket);
  if (CF.zcontrol_socket) zmq_close(CF.zcontrol_socket);
  if (CF.zmq_context) zmq_term(CF.zmq_context);
  if (CF.spool) free(CF.spool);
  if (CF.s) utstring_free(CF.s);
  if (CF.frames_1h) ts_free(CF.frames_1h);
}

