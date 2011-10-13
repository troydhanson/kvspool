#include <inttypes.h>
#include <pthread.h>
#include "uthash.h"
#include "ts.h"
#include "tracker.h"

#define URI_BUCKETS 64
#define LN_URI_BUCKETS 6

typedef struct {
  char ip[16];  // key
  ts_t *hits_1h;
  ts_t *maps_1h;
  ts_t *hits_1w;
  ts_t *etop_1w;
  time_t last;
  UT_hash_handle hh;
} user_t;

typedef struct {
  user_t *users;
  // enterprise
  ts_t *hits_1h;
  ts_t *maps_1h;
  ts_t *hits_1w;
  tracker_t *uri_tracker;
  time_t last;
} kvz_t;


struct _CF {
  kvz_t *kvz;
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
  int walltime_mode;
  void *zcontrol;
  UT_string *s; // scratch space
};

#define adim(x) (sizeof(x)/sizeof(*x))
#define ZCON_DEF_ENDPOINT "tcp://127.0.0.1:3333"

