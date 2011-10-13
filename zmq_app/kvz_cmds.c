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

extern struct _CF CF;

/* some control port commands */
int shutdown_cmd(void *cp, cp_arg_t *arg, void *data) {
  cp_add_replys(cp,"shutting down");
  pthread_kill(CF.spr_thread,SIGINT);
}

int stats_cmd(void *cp, cp_arg_t *arg, void *data) {
  utstring_renew(CF.s);
  utstring_printf(CF.s, "frames_read:      %u\n", CF.frames_read);
  utstring_printf(CF.s, "frames_processed: %u\n", CF.frames_processed);
  cp_add_reply(cp,utstring_body(CF.s),utstring_len(CF.s));
}

int rates_cmd(void *cp, cp_arg_t *arg, void *data) {
  utstring_renew(CF.s);
  unsigned i,hits;
  char *when;
  for(i=0; i<CF.frames_1h->num_buckets; i++) {
    hits = *(unsigned*)(bkt(CF.frames_1h,i)->data);
    when = asctime(localtime(&(bkt(CF.frames_1h,i)->start)));
    utstring_printf(CF.s, " %u frames: %s", hits, when);
  }
  cp_add_reply(cp,utstring_body(CF.s),utstring_len(CF.s));
}

cp_cmd_t cmds[] = { 
  {"stats",      stats_cmd,      "server stats"},
  {"shutdown",   shutdown_cmd,   "shutdown server"},
  {"rates",      rates_cmd,   "ingest rates over 1h"},
  {NULL, NULL, NULL},
};

