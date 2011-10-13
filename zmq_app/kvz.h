#include <inttypes.h>
#include <pthread.h>
#include "utstring.h"
#include "ts.h"

struct _CF {
  ts_t *frames_1h;
  int verbose;
  int frames_read;
  int frames_processed;
  int controls;
  char *spool;
  int exit_when_spool_consumed; // for testing, instead of blocking 
  void *zmq_context;
  void *spr_push_socket;
  void *spr_pull_socket;
  void *zcontrol_socket;
  char *zcontrol_endpoint; // e.g. tcp://eth0:3333
  pthread_t spr_thread;
  int request_exit;
  void *zcontrol;
  UT_string *s; /* scratch */
};

#define adim(x) (sizeof(x)/sizeof(*x))
#define ZCON_DEF_ENDPOINT "tcp://127.0.0.1:3333"

