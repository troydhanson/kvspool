#include <errno.h>
#include <assert.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>
#include <nanomsg/nn.h>
#include <nanomsg/pipeline.h>
#include <nanomsg/reqrep.h>
#include <pthread.h>
#include <limits.h>
#include <time.h>
#include <jansson.h>
#include <librdkafka/rdkafka.h>
#include "utarray.h"
#include "utstring.h"
#include "libnnctl.h"
#include "kvspool.h"
#include "tpl.h"
#include "ts.h"

struct {
  int verbose;
  int shutdown;
  char *dir;
  char *prog;
  int signal_fd;
  int epoll_fd;
  int ticks;
  time_t now;
  /* stats */
  ts_t *spr_msgs_ts;
  ts_t *kaf_msgs_ts;
  ts_t *kaf_bytes_ts;
  /* remote receiver */
  char *broker;
  char *topic;
  UT_array *rdkafka_options;
  UT_array *rdkafka_topic_options;
  /* threads */
  int nthread;
  pthread_t spr_thread;  /* spool reader thread (1) */
  pthread_t *enc_thread; /* json encoding threads (n) */
  pthread_t *kaf_thread; /* kafkaa sending threads(n) */
  /* nano stuff below */
  int ingress_socket_push;
  int ingress_socket_pull;
  int egress_socket_push;
  int egress_socket_pull;
  char *nnctl_binding;
  int nnctl_socket;
  int nnctl_fd;
  nnctl *nnctl;
} CF = {
  .signal_fd = -1,
  .epoll_fd = -1,
  .ingress_socket_push = -1,
  .ingress_socket_pull = -1,
  .egress_socket_push = -1,
  .egress_socket_pull = -1,
  .nnctl_binding = "tcp://127.0.0.1:9995",
  .nnctl_socket = -1,
  .nthread = 1,
};

#define STATS_INTERVAL 10
const ts_mm ts_int_mm = {.sz=sizeof(int)};

void usage() {
  fprintf(stderr,"usage: %s <options>\n" 
                 "\n"
                 " options:\n"
                 "   -v               (verbose)\n"
                 "   -d <spool>       (spool)\n"
                 "   -t <topic>       (kafka topic)\n"
                 "   -b <broker>      (kafka broker)\n"
                 "   -N <binding>     (nnctl binding)\n"
                 "   -n <nthread>     (threads/pool) [def:1]\n"
                 "\n"
                 "The rdkafka config variables should be left at default values!\n"
                 "\n"
                 "   -c option=value  (rdkafka options, repeatable)\n"
                 "        statistics.interval.ms=60000 (0=disables)\n"
                 "        compression.codec=gzip (none, gzip, snappy)\n"
                 "        delivery.report.only.error=true\n"
                 "        ...\n"
                 "   -C option=value  (rdkafka topic options, repeatable)\n"
                 "        request.required.acks=0 (0=no acks, 1=ack)\n"
                 "        message.timeout.ms=0 (millisec waited for successful delivery, 0=infinite)\n"
                 "        ...\n"
                 "\n",
                 CF.prog);
  exit(-1);
}

/* signals that we'll accept via signalfd in epoll */
int sigs[] = {SIGHUP,SIGTERM,SIGINT,SIGQUIT,SIGALRM};

double rate_per_sec(ts_t *t) {
  long total=0;
  int i;
  for(i=0; i<t->num_buckets; i++) total += *(int*)(bkt(t,i)->data);
  int report_seconds = STATS_INTERVAL * t->num_buckets;
  double events_per_second = report_seconds ? (total * 1.0 / report_seconds) : 0;
  return events_per_second;
}

void periodic_work() {
  fprintf(stderr,"i/o summary\n");
  fprintf(stderr," spool read rate:  %f msgs/sec\n", rate_per_sec(CF.spr_msgs_ts));
  fprintf(stderr," xmitr#1 intake rate:  %f msgs/sec\n", rate_per_sec(CF.kaf_msgs_ts));
  fprintf(stderr," xmitr#1 output rate: %f bytes/sec\n", rate_per_sec(CF.kaf_bytes_ts));
}

int new_epoll(int events, int fd) {
  int rc;
  struct epoll_event ev;
  memset(&ev,0,sizeof(ev)); // placate valgrind
  ev.events = events;
  ev.data.fd= fd;
  if (CF.verbose) fprintf(stderr,"adding fd %d to epoll\n", fd);
  rc = epoll_ctl(CF.epoll_fd, EPOLL_CTL_ADD, fd, &ev);
  if (rc == -1) {
    fprintf(stderr,"epoll_ctl: %s\n", strerror(errno));
  }
  return rc;
}

int handle_signal() {
  int rc=-1;
  struct signalfd_siginfo info;
  
  if (read(CF.signal_fd, &info, sizeof(info)) != sizeof(info)) {
    fprintf(stderr,"failed to read signal fd buffer\n");
    goto done;
  }

  switch(info.ssi_signo) {
    case SIGALRM: 
      if (CF.shutdown) goto done;
      CF.now = time(NULL);
      if ((++CF.ticks % STATS_INTERVAL) == 0) periodic_work();
      alarm(1); 
      break;
    default: 
      fprintf(stderr,"got signal %d\n", info.ssi_signo);  
      goto done;
      break;
  }

 rc = 0;

 done:
  return rc;
}


#define MAX_BUF 65536
/* encode a tpl (kv frame) as json. */
void *enc_worker(void *thread_id) {
  char buf[MAX_BUF], *key, *val;
  json_t *o = json_object();
  int rc=-1, len, nc;
  tpl_node *tn;

  while (CF.shutdown == 0) {
    len = nn_recv(CF.ingress_socket_pull, buf, MAX_BUF, 0);
    if (len < 0) {
      fprintf(stderr,"nn_recv: %s\n", nn_strerror(errno));
      goto done;
    }
    /* decode, then re-encode as json */
    json_object_clear(o);
    tn = tpl_map("A(ss)",&key,&val); assert(tn);
    if (tpl_load(tn,TPL_MEM,buf,len) < 0) goto done;
    while(tpl_unpack(tn,1) > 0) {
      json_t *jval = json_string(val);
      json_object_set_new(o, key, jval); 
      free(key); key=NULL;
      free(val); val=NULL;
    }
    tpl_free(tn);

    /* dump the json object, then newline-terminate it. */
    if (CF.verbose>1) json_dumpf(o, stderr, JSON_INDENT(1));
    char *dump = json_dumps(o, JSON_INDENT(0));
    size_t dump_len = strlen(dump);

    /* give the buffer to nano, from here it goes to kaf thread */
    nc = nn_send(CF.egress_socket_push, dump, dump_len, 0);
    free(dump);
    if (nc < 0) {
      fprintf(stderr,"nn_send: %s\n", nn_strerror(errno));
      goto done;
    }
  }

  rc = 0;

 done:
  CF.shutdown = 1;
  json_decref(o);
  return NULL;
}

static void err_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
  fprintf(stderr,"%% ERROR CALLBACK: %s: %s: %s\n",
    rd_kafka_name(rk), rd_kafka_err2str(err), reason);
  CF.shutdown=1;
}

static void throttle_cb (rd_kafka_t *rk, const char *broker_name,
                         int32_t broker_id, int throttle_time_ms,
                         void *opaque) {
  fprintf(stderr,"%% THROTTLED %dms by %s (%"PRId32")\n", throttle_time_ms,
               broker_name, broker_id);
}

static int stats_cb (rd_kafka_t *rk, char *json, size_t json_len,
                     void *opaque) {

        /* Extract values for our own stats */
        //json_parse_stats(json);

        //if (stats_fp)
                //fprintf(stats_fp, "%s\n", json);
   fprintf(stderr, "stats:\n%.*s\n", (int)json_len, json);
   return 0;
}




/* transmitter worker */
void *kaf_worker(void *thread_id) {
  char buf[MAX_BUF], *b;
  int rc=-1, nr, len, l, count=0,kr;

  /* kafka connection setup */
  char errstr[512];
  rd_kafka_t *k;
  rd_kafka_topic_t *t;
  rd_kafka_conf_t *conf;
  rd_kafka_topic_conf_t *topic_conf;
  int partition = RD_KAFKA_PARTITION_UA;
  char *key = NULL;
  int keylen = key ? strlen(key) : 0;

  /* set up global options */
  conf = rd_kafka_conf_new();
  rd_kafka_conf_set_error_cb(conf, err_cb);
  rd_kafka_conf_set_throttle_cb(conf, throttle_cb);
  rd_kafka_conf_set_stats_cb(conf, stats_cb);
  kr = rd_kafka_conf_set(conf, "statistics.interval.ms", "60000", errstr, sizeof(errstr));
  if (kr != RD_KAFKA_CONF_OK) {
    fprintf(stderr,"error: rd_kafka_conf_set: statistics.interval.ms 60000 => %s\n", errstr);
    goto done;
  }
  char **opt=NULL;
  while( (opt=(char **)utarray_next(CF.rdkafka_options,opt))) {
    char *eq = strchr(*opt,'=');
    if (eq == NULL) {
      fprintf(stderr,"error: specify rdkafka params as key=value\n");
      goto done;
    }
    char *k = strdup(*opt), *v;
    k[eq-*opt] = '\0';
    v = &k[eq-*opt + 1];
    if (CF.verbose) fprintf(stderr,"setting %s %s\n", k, v); 
    kr = rd_kafka_conf_set(conf, k, v, errstr, sizeof(errstr));
    if (kr != RD_KAFKA_CONF_OK) {
      fprintf(stderr,"error: rd_kafka_conf_set: %s %s => %s\n", k, v, errstr);
      goto done;
    }
    free(k);
  }

  
  /* set up topic options */
  topic_conf = rd_kafka_topic_conf_new();
  opt=NULL;
  while( (opt=(char **)utarray_next(CF.rdkafka_topic_options,opt))) {
    char *eq = strchr(*opt,'=');
    if (eq == NULL) {
      fprintf(stderr,"error: specify rdkafka topic params as key=value\n");
      goto done;
    }
    char *k = strdup(*opt), *v;
    k[eq-*opt] = '\0';
    v = &k[eq-*opt + 1];
    if (CF.verbose) fprintf(stderr,"setting %s %s\n", k, v); 
    kr = rd_kafka_topic_conf_set(topic_conf, k, v, errstr, sizeof(errstr));
    if (kr != RD_KAFKA_CONF_OK) {
      fprintf(stderr,"error: rd_kafka_conf_set: %s %s => %s\n", k, v, errstr);
      goto done;
    }
    free(k);
  }


  k = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  if (k == NULL) {
    fprintf(stderr, "rd_kafka_new: %s\n", errstr);
    goto done;
  }

  if (rd_kafka_brokers_add(k, CF.broker) < 1) {
    fprintf(stderr, "invalid broker\n");
    goto done;
  }

  t = rd_kafka_topic_new(k, CF.topic, topic_conf);

  while (CF.shutdown == 0) {
    len = nn_recv(CF.egress_socket_pull, buf, MAX_BUF, 0);
    if (len < 0) {
      fprintf(stderr,"nn_recv: %s\n", nn_strerror(errno));
      goto done;
    }

    rc = rd_kafka_produce(t, partition, RD_KAFKA_MSG_F_COPY, buf, len, 
                     key, keylen, NULL);
    if (rc == -1) {
      fprintf(stderr,"rd_kafka_produce: %s %s\n", 
        rd_kafka_err2str( rd_kafka_errno2err(errno)), 
        ((errno == ENOBUFS) ? "(backpressure)" : ""));
      goto done;
    }

    // cause rdkafka to invoke optional callbacks (msg delivery reports or error)
    if ((++count % 1000) == 0) rd_kafka_poll(k, 0);
    
    if (thread_id == 0) {
      /* only emit these stats from the first worker thread (not thread safe) */
      ts_add(CF.kaf_bytes_ts, CF.now, &len);
      ts_add(CF.kaf_msgs_ts, CF.now, NULL);
    }
  }

  rc = 0;

 done:
  CF.shutdown = 1;
  return NULL;
}


/* this returns memory that caller must free */
int set_to_buf(void *set, char **buf, size_t *len) {
  int rc=-1;
  char *key, *val;
  tpl_node *tn = tpl_map("A(ss)",&key,&val); assert(tn);
  kv_t *kv = NULL;
  while (kv = kv_next(set,kv)) {
    key = kv->key;
    val = kv->val;
    tpl_pack(tn,1);
  }
  if (tpl_dump(tn,TPL_MEM,buf,len) < 0) goto done;
  tpl_free(tn);

  rc = 0;

 done:
  return rc;
}

void *spr_worker(void *data) {
  int rc=-1, nc;
  void *sp=NULL, *set;
  size_t len;
  char *buf;

  set = kv_set_new();
  sp = kv_spoolreader_new(CF.dir);
  if (!sp) goto done;

  while (kv_spool_read(sp,set,1) > 0) {
    if (set_to_buf(set,&buf,&len) < 0) goto done;
    assert(len < MAX_BUF);
    nc = nn_send(CF.ingress_socket_push, buf, len, 0);
    free(buf);
    if (nc < 0) {
      fprintf(stderr,"nn_send: %s\n", nn_strerror(errno));
      goto done;
    }
    ts_add(CF.spr_msgs_ts, CF.now, NULL);
    if (CF.shutdown) break;
  }

 done:
  CF.shutdown = 1;
  kv_set_free(set);
  if (sp) kv_spoolreader_free(sp);
  return NULL;
}

/* we use nano sockets to set up a pipeline like:
 *
 *   spool-reader
 *       PUSH
 *         |   <--- "ingress" socket
 *       PULL
 *    json-encoder
 *       PUSH
 *         |   <--- "egress" socket
 *       PULL
 *     kaf-sender
 */
int setup_nano(void) {
  int rc = -1;

  /* set up the ingress and egress sockets */
  if ( (CF.ingress_socket_push = nn_socket(AF_SP, NN_PUSH)) < 0) goto done;
  if ( (CF.ingress_socket_pull = nn_socket(AF_SP, NN_PULL)) < 0) goto done;
  if ( (CF.egress_socket_push  = nn_socket(AF_SP, NN_PUSH)) < 0) goto done;
  if ( (CF.egress_socket_pull  = nn_socket(AF_SP, NN_PULL)) < 0) goto done;

  if (nn_bind(CF.ingress_socket_push,    "ipc://ingress.ipc") < 0) goto done;
  if (nn_connect(CF.ingress_socket_pull, "ipc://ingress.ipc") < 0) goto done;

  if (nn_bind(CF.egress_socket_push,     "ipc://egress.ipc") < 0) goto done;
  if (nn_connect(CF.egress_socket_pull,  "ipc://egress.ipc") < 0) goto done;

  /* set up our nnctl rep socket */
  if ( (CF.nnctl_socket  = nn_socket(AF_SP, NN_REP)) < 0) goto done;
  if (nn_bind(CF.nnctl_socket, CF.nnctl_binding) < 0) goto done;
  if ( (CF.nnctl = nnctl_init(NULL, NULL)) == NULL) goto done;

  rc = 0;

 done:
  if (rc < 0) fprintf(stderr,"nano: %s\n", nn_strerror(errno));
  return rc;
}

int main(int argc, char *argv[]) {
  int opt, i, n, rc=-1;
  struct epoll_event ev;
  CF.prog = argv[0];
  CF.now = time(NULL);
  utarray_new(CF.rdkafka_options,&ut_str_icd);
  utarray_new(CF.rdkafka_topic_options,&ut_str_icd);
  void *res;

  while ( (opt = getopt(argc, argv, "hv+N:n:d:b:t:c:C:")) != -1) {
    switch(opt) {
      case 'v': CF.verbose++; break;
      case 'n': CF.nthread=atoi(optarg); break;
      case 'N': CF.nnctl_binding=strdup(optarg); break;
      case 'd': CF.dir=strdup(optarg); break;
      case 't': CF.topic=strdup(optarg); break;
      case 'b': CF.broker=strdup(optarg); break;
      case 'c': utarray_push_back(CF.rdkafka_options,&optarg); break;
      case 'C': utarray_push_back(CF.rdkafka_topic_options,&optarg); break;
      case 'h': default: usage();
    }
  }
  if (CF.dir == NULL) usage();
  if (CF.broker == NULL) usage();
  if (CF.topic == NULL) CF.topic = CF.dir;

  /* stats (time series) for input/output tracking */
  CF.spr_msgs_ts = ts_new(6,  STATS_INTERVAL ,&ts_int_mm);
  CF.kaf_msgs_ts = ts_new(6,  STATS_INTERVAL, &ts_int_mm);
  CF.kaf_bytes_ts = ts_new(6, STATS_INTERVAL, &ts_int_mm);

  /* block all signals. we take signals synchronously via signalfd */
  sigset_t all;
  sigfillset(&all);
  sigprocmask(SIG_SETMASK,&all,NULL);

  /* a few signals we'll accept via our signalfd */
  sigset_t sw;
  sigemptyset(&sw);
  for(n=0; n < sizeof(sigs)/sizeof(*sigs); n++) sigaddset(&sw, sigs[n]);

  /* create the signalfd for receiving signals */
  CF.signal_fd = signalfd(-1, &sw, 0);
  if (CF.signal_fd == -1) {
    fprintf(stderr,"signalfd: %s\n", strerror(errno));
    goto done;
  }

  /* set up the epoll instance */
  CF.epoll_fd = epoll_create(1); 
  if (CF.epoll_fd == -1) {
    fprintf(stderr,"epoll: %s\n", strerror(errno));
    goto done;
  }

  if (setup_nano() < 0) goto done;

  /* add descriptors of interest */
  size_t sz = sizeof(CF.nnctl_fd);
  nn_getsockopt(CF.nnctl_socket, NN_SOL_SOCKET, NN_RCVFD, &CF.nnctl_fd, &sz);
  if (new_epoll(EPOLLIN, CF.nnctl_fd)) goto done;  // nnctl socket
  if (new_epoll(EPOLLIN, CF.signal_fd)) goto done; // signal socket

  /* fire up threads */
  rc = pthread_create(&CF.spr_thread, NULL, spr_worker, NULL); if (rc) goto done;
  CF.enc_thread = malloc(sizeof(pthread_t)*CF.nthread);
  if (CF.enc_thread == NULL) goto done;
  long id;
  for(i=0; i < CF.nthread; i++) {
    id = i;
    rc = pthread_create(&CF.enc_thread[i],NULL,enc_worker,(void*)id);
    if (rc) goto done;
  }

  CF.kaf_thread = malloc(sizeof(pthread_t)*CF.nthread);
  if (CF.kaf_thread == NULL) goto done;
  for(i=0; i < CF.nthread; i++) {
    id = i;
    rc = pthread_create(&CF.kaf_thread[i],NULL,kaf_worker,(void*)id);
    if (rc) goto done;
  }

  alarm(1);
  while (epoll_wait(CF.epoll_fd, &ev, 1, -1) > 0) {
    if (ev.data.fd == CF.signal_fd) { 
      if (handle_signal() < 0) goto done; 
    }
    if (ev.data.fd == CF.nnctl_fd)  { 
      if (nnctl_exec(CF.nnctl, CF.nnctl_socket) < 0) goto done; 
    }
  }

  rc = 0;

done:
  CF.shutdown=1;
  nn_term();
  fprintf(stderr,"shutting down threads:\n");

  fprintf(stderr,"spoolreader...\n");
  if (CF.spr_thread) {
    pthread_cancel(CF.spr_thread);
    pthread_join(CF.spr_thread,NULL);
  }

  fprintf(stderr,"encoders...\n");
  if (CF.enc_thread) {
    for(i=0; i < CF.nthread; i++) {
      pthread_cancel(CF.enc_thread[i]);
      pthread_join(CF.enc_thread[i],NULL);
    }
  }

  fprintf(stderr,"transmitters...\n");
  if (CF.kaf_thread) {
    for(i=0; i < CF.nthread; i++) {
      pthread_cancel(CF.kaf_thread[i]);
      pthread_join(CF.kaf_thread[i],NULL);
    }
  }

  fprintf(stderr,"terminating...\n");
  if (CF.ingress_socket_push >= 0) nn_close(CF.ingress_socket_push);
  if (CF.ingress_socket_pull >= 0) nn_close(CF.ingress_socket_pull);
  if (CF.egress_socket_push >= 0) nn_close(CF.egress_socket_push);
  if (CF.egress_socket_pull >= 0) nn_close(CF.egress_socket_pull);
  if (CF.nnctl_socket >= 0) nn_close(CF.nnctl_socket);
  if (CF.epoll_fd != -1) close(CF.epoll_fd);
  if (CF.signal_fd != -1) close(CF.signal_fd);
  ts_free(CF.spr_msgs_ts);
  ts_free(CF.kaf_msgs_ts);
  ts_free(CF.kaf_bytes_ts);
  if (CF.enc_thread) free(CF.enc_thread);
  if (CF.kaf_thread) free(CF.kaf_thread);
  if (CF.nnctl) nnctl_free(CF.nnctl);
  utarray_free(CF.rdkafka_options);
  utarray_free(CF.rdkafka_topic_options);
  return rc;
}
