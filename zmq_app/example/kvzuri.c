#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#include <zmq.h>
#include "kvspool.h"
#include "zcontrol.h"
#include "utarray.h"
#include "kvzuri.h"

struct _CF CF = {
  .zcontrol_endpoint = ZCON_DEF_ENDPOINT,
};

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

static unsigned hash_ber(char *in) {
  unsigned hashv = 0;
  while (*in != '\0')  hashv = ((hashv) * 33) + *in++;
  return hashv;
}

#define bucket(u) (u & ((1 << LN_URI_BUCKETS) - 1))
static void uri_hash_incr(int *urimap, char *uri) {
  unsigned uri_hash = hash_ber(uri);
  urimap[bucket(uri_hash)]++;
}
// want to use renew here (no dtor) but then there's no way to do a final 'done' since dtor is on every ts slide
static void etop_ctor(UT_array *a, size_t sz) {utarray_init(a,&ut_int_icd);}
static void etop_dtor(UT_array *a) {utarray_done(a);}
static int intcmp(const void *_a, const void *_b) {int a=*(int *)_a; int b=*(int*)_b; return a-b;}
#define MAX_DAILY_URI_PER_IP 1000
static void etop_add(UT_array *a, char *uri) {
  // add uniquely; if not found; add, sort
  if (utarray_len(a) >= MAX_DAILY_URI_PER_IP) return;
  int uri_hash = hash_ber(uri);
  if (utarray_find(a,&uri_hash,intcmp)) return;
  utarray_push_back(a,&uri_hash);
  utarray_sort(a,intcmp);
}
ts_mm hits_1h_mm = {.sz=sizeof(int)};
ts_mm hits_1w_mm = {.sz=sizeof(int)};
ts_mm maps_1h_mm = {.sz=sizeof(int)*URI_BUCKETS,.data=(ts_data_f*)uri_hash_incr};
ts_mm etop_1w_mm = {.sz=sizeof(UT_array),.data=(ts_data_f*)etop_add,.ctor=(ts_ctor_f*)etop_ctor,.dtor=(ts_dtor_f*)etop_dtor};

kvz_t *new_kvz(void) {  
  kvz_t *c = malloc(sizeof(kvz_t));
  memset(c,0,sizeof(*c));
  c->hits_1h = ts_new(60/5,5*60,&hits_1h_mm);
  c->maps_1h = ts_new(60,60,&maps_1h_mm);
  c->hits_1w = ts_new(24*7,60*60,&hits_1w_mm);
  c->uri_tracker = tracker_new(100000,10);
  return c;
}

void free_kvz(kvz_t *c) {
  user_t *u, *tmp;
  HASH_ITER(hh, c->users, u, tmp) {
    HASH_DEL(c->users, u);
    ts_free(u->hits_1h);
    ts_free(u->maps_1h);
    ts_free(u->hits_1w);
    ts_free(u->etop_1w);
    free(u);
  }
  ts_free(c->hits_1h);
  ts_free(c->maps_1h);
  ts_free(c->hits_1w);
  tracker_free(c->uri_tracker);
  free(c);
}

void add_hit(kvz_t *c, char *ip, char *when, char *uri) {
  user_t user,*u;
  time_t t = atol(when);
  HASH_FIND(hh, c->users, ip, strlen(ip), u);
  if (!u) {
    u = calloc(1,sizeof(user_t));
    if (!u) {fprintf(stderr,"oom\n"); return;}
    strncpy(u->ip, ip, sizeof(u->ip));
    u->hits_1h = ts_new(60/5,5*60,&hits_1h_mm);
    u->maps_1h = ts_new(60,60,&maps_1h_mm);
    u->hits_1w = ts_new(24*7,60*60,&hits_1w_mm);
    u->etop_1w = ts_new(7,24*60*60,&etop_1w_mm);
    u->last = t;
    HASH_ADD(hh, c->users, ip, strlen(ip), u);
  }
  ts_add(u->hits_1h, t, NULL);
  ts_add(u->maps_1h, t, uri);
  ts_add(u->hits_1w, t, NULL);
  ts_add(u->etop_1w, t, uri);
  // enterprise
  ts_add(c->hits_1h, t, NULL);
  ts_add(c->maps_1h, t, uri);
  ts_add(c->hits_1w, t, NULL);
  tracker_hit(c->uri_tracker, uri, t);
  c->last = t;
}

void show_version(char *prog) {
  fprintf(stderr, "%s 0.1\n", prog);
  int major, minor, patch;
  zmq_version (&major, &minor, &patch);
  fprintf(stderr,"Current 0MQ version is %d.%d.%d\n", major, minor, patch);
  exit(-1);
}

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] [-s spool] [-x] [-w] [-V]\n", prog);
  exit(-1);
}

// this is for development when reading a historical web spool
// it adds delays as needed to read at the original event rate
time_t last_frame_ts, last_frame_wc;
void walltime_delay(void *set) {
  time_t cur_frame_ts, now=time(NULL);
  kv_t *ts = kv_get(set,"ts"); if (!ts) return;
  cur_frame_ts = atol(ts->val);
  if (last_frame_ts==0) last_frame_ts=cur_frame_ts;
  if (last_frame_wc==0) last_frame_wc=now;
  time_t frame_elapsed = (cur_frame_ts-last_frame_ts);
  time_t wallt_elapsed = (now-last_frame_wc);
  if (frame_elapsed > wallt_elapsed) {
    time_t delay = frame_elapsed-wallt_elapsed;
    //fprintf(stderr,"delaying %lu\n",(long)delay);
    sleep(delay);
  }
  last_frame_ts = cur_frame_ts;
  last_frame_wc = now;
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
    if (CF.walltime_mode) walltime_delay(set);
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
        kv_t *sip  = kv_get(set, "sip");
        kv_t *when = kv_get(set, "ts");
        kv_t *uri  = kv_get(set, "host");
        if (sip && when && uri ) {
          add_hit(CF.kvz,sip->val,when->val,uri->val);
        }
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

  while ( (opt = getopt(argc, argv, "v+xs:Vq:w")) != -1) {
    switch (opt) {
      case 'v': CF.verbose++; break;
      case 'V': show_version(argv[0]); break;
      case 's': CF.spool=strdup(optarg); break;
      case 'x': CF.exit_when_spool_consumed=1; break;
      case 'q': CF.zcontrol_endpoint=strdup(optarg); break;
      case 'w': CF.walltime_mode=1; break;
      default: usage(argv[0]); break;
    }
  }

  if (!CF.spool) usage(argv[0]);
  if (setup_zmq() == -1) goto program_exit;

  CF.kvz = new_kvz();
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
  if (CF.kvz) free_kvz(CF.kvz);
  if (CF.s) utstring_free(CF.s);
}

