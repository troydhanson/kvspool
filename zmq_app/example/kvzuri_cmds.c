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

void print_int_buckets(ts_t *t, UT_string *s) {
  int i;
  utstring_printf(s,"[ ");
  for(i=0; i < t->num_buckets; i++) {
    utstring_printf(s, "%c%u", (i?',':' '),*(unsigned*)(bkt(t,i)->data));
  }
  utstring_printf(s,"],\n ");
}

void print_top(tracker_t *t, UT_string *s) {
  uri_t **up=NULL,*u; int i=0;
  utstring_printf(s,"{ ");
  while( (up=(uri_t**)utarray_next(&t->top,up))) {
    u = *up;
    utstring_printf(s, "%c%s: [%u,%lu] ", (i++?',':' '),
      utstring_body(&u->uri), u->count, (long)u->last);
  }
  utstring_printf(s," },\n");
}

// slot: 60 buckets each 60 sec long (1hour). 
// each bucket contains a uri hash histogram
void print_map(ts_t *t, UT_string *s) { 
  int i,j;
  unsigned sum[URI_BUCKETS], bkt_count, total=0;
  memset(sum,0,sizeof(int)*URI_BUCKETS);
  for(i=0; i < t->num_buckets; i++) {
    for(j=0; j < URI_BUCKETS; j++) {
      bkt_count = ((unsigned*)(bkt(t,i)->data))[j];
      sum[j] += bkt_count;
      total += bkt_count;
    }
  }
  utstring_printf(s, "[ ");
  for(j=0; j < URI_BUCKETS; j++) {
    utstring_printf(s, "%c%.3f", (j?',':' '), sum[j]*1.0/total);
  }
  utstring_printf(s, " ],\n");
}

int enterprise_cmd(void *cp, cp_arg_t *arg, void *data) {
  utstring_renew(CF.s);
  utstring_printf(CF.s, "{\n");
  utstring_printf(CF.s, "  \"last\": %lu,\n", (long)CF.kvz->last);
  utstring_printf(CF.s, "  \"hits_1h\": "); print_int_buckets(CF.kvz->hits_1h,CF.s);
  utstring_printf(CF.s, "  \"hits_1w\": "); print_int_buckets(CF.kvz->hits_1w,CF.s);
  utstring_printf(CF.s, "  \"maps_1h\": "); print_map(CF.kvz->maps_1h,CF.s);
  utstring_printf(CF.s, "  \"top\": "); print_top(CF.kvz->uri_tracker,CF.s);
  utstring_printf(CF.s, "}\n");
  cp_add_reply(cp,utstring_body(CF.s),utstring_len(CF.s));
}

int user_cmd(void *cp, cp_arg_t *arg, void *data) {
  user_t *u;
  char *user_ip;

  if (arg->argc < 2) { cp_add_replys(cp,"no user specified"); return; }

  // if user is '-' just grab the last one (most recent insert) for testing
  user_ip = arg->argv[1];
  if (*user_ip=='-') u=CF.kvz->users;
  else HASH_FIND_STR(CF.kvz->users, user_ip, u);

  if (!u) { cp_add_replys(cp,"no such user"); return; }

  utstring_renew(CF.s);
  utstring_printf(CF.s, "{\n");
  utstring_printf(CF.s, "  \"user\": \"%s\",\n", u->ip);
  utstring_printf(CF.s, "  \"last\": %lu,\n", (long)u->last);
  utstring_printf(CF.s, "  \"hits_1h\": "); print_int_buckets(u->hits_1h,CF.s);
  utstring_printf(CF.s, "  \"hits_1w\": "); print_int_buckets(u->hits_1w,CF.s);
  utstring_printf(CF.s, "  \"maps_1h\": "); print_map(u->maps_1h,CF.s);
  utstring_printf(CF.s, "  \"top\": \"TODO\"\n");
  utstring_printf(CF.s, "}\n");
  cp_add_reply(cp,utstring_body(CF.s),utstring_len(CF.s));
}

cp_cmd_t cmds[] = { 
  {"stats",      stats_cmd,      "server stats"},
  {"user",       user_cmd,       "get user data"},
  {"enterprise", enterprise_cmd, "enterprise server"},
  {"shutdown",   shutdown_cmd,   "shutdown server"},
  {NULL, NULL, NULL},
};

