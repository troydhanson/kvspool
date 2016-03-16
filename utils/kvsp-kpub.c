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
#include <jansson.h>
#include <librdkafka/rdkafka.h>
#include "kvspool.h"

int verbose;
char *spool;
char *broker;
char *topic;
char errstr[512];

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] -d spool <broker>\n", prog);
  exit(-1);
}

int main(int argc, char *argv[]) {
  void *sp=NULL;
  void *set=NULL;
  int opt,rc=-1;
  json_t *o = NULL;
  int ticks=0;

  while ( (opt = getopt(argc, argv, "v+d:st:")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'd': spool=strdup(optarg); break;
      case 't': topic=strdup(optarg); break;
      default: usage(argv[0]); break;
    }
  }
  if (optind < argc) broker = argv[optind++];
  if (broker == NULL) usage(argv[0]);
  if (spool == NULL) usage(argv[0]);
  if (topic == NULL) topic = spool;

  set = kv_set_new();
  sp = kv_spoolreader_new(spool);
  if (!sp) goto done;
  o = json_object();

  rd_kafka_t *k;
  rd_kafka_topic_t *t;
  rd_kafka_conf_t *conf;
  rd_kafka_topic_conf_t *topic_conf;
  int partition = RD_KAFKA_PARTITION_UA;
  char *key = NULL;
  int keylen = key ? strlen(key) : 0;

  conf = rd_kafka_conf_new();
  topic_conf = rd_kafka_topic_conf_new();

  k = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  if (k == NULL) {
    fprintf(stderr, "rd_kafka_new: %s\n", errstr);
    goto done;
  }
  if (rd_kafka_brokers_add(k, broker) < 1) {
    fprintf(stderr, "invalid broker\n");
    goto done;
  }

  t = rd_kafka_topic_new(k, topic, topic_conf);

  while (kv_spool_read(sp,set,1) > 0) { /* read til interrupted by signal */
    json_object_clear(o);
    kv_t *kv = NULL;
    while (kv = kv_next(set,kv)) {
      json_t *jval = json_string(kv->val);
      json_object_set_new(o, kv->key, jval); 
    }
    if (verbose) json_dumpf(o, stderr, JSON_INDENT(1));


    char *json  = json_dumps(o, JSON_INDENT(1));
    size_t len = strlen(json);

    rc = rd_kafka_produce(t, partition, RD_KAFKA_MSG_F_FREE, json, len, 
                     key, keylen, NULL);

    if ((rc == -1) && (errno == ENOBUFS)) {
      /* check for backpressure. what to do? wait for space.. */
      fprintf(stderr,"backpressure\n");
      goto done; // FIXME
    }

    if (rc == -1) {
      fprintf(stderr,"rd_kafka_produce: failed\n");
      goto done;
    }

    if (++ticks % 1000) rd_kafka_poll(k, 10); // FIXME handle delivery reports
  }

  rc = 0;

 done:
  if (sp) kv_spoolreader_free(sp);
  kv_set_free(set);
  if(o) json_decref(o);

  return 0;
}

