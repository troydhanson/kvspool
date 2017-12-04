#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <time.h>
#include <nanomsg/nn.h>
#include <nanomsg/pipeline.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>


#include "kvspool_internal.h"
#include "utstring.h"
#include "uthash.h"
#include "kvsp-bconfig.h"

char *config_file;
UT_string *tmp;
int sock,eid;
int verbose;
char *local;
char *dir;
void *set;
void *sp;

void usage(char *exe) {
  fprintf(stderr,"usage: %s [-v] -b <cast-config> -d <dir> <listen-addr>\n", exe);
  fprintf(stderr," <listen-addr> is our local address e.g. tcp://0.0.0.0:1234\n");
  exit(-1);
}

int main(int argc, char *argv[]) {
  int opt,rc=-1,len;
  char *buf;

  set = kv_set_new();
  utstring_new(tmp);
  utarray_new(output_keys, &ut_str_icd);
  utarray_new(output_defaults, &ut_str_icd);
  utarray_new(output_types,&ut_int_icd);

  while ( (opt = getopt(argc, argv, "d:b:v+")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'd': dir=strdup(optarg); break;
      case 'b': config_file=strdup(optarg); break;
      default: usage(argv[0]); break;
    }
  }

  if (!dir) usage(argv[0]);
  if (parse_config(config_file) < 0) goto done;
  if ( !(sp = kv_spoolwriter_new(dir))) goto done;
  if (optind < argc) local = argv[optind++];
  if (!local) usage(argv[0]);
  rc = -2;

  if ( (sock = nn_socket(AF_SP, NN_PULL)) < 0) goto done;
  if ( (eid = nn_bind(sock, local)) < 0) goto done;

  while(1) {

    if ( (len = nn_recv(sock, &buf, NN_MSG, 0)) == -1) goto done;
    if (binary_to_frame(sp,set,buf,len,tmp)) {rc=-3; goto done;}
    nn_freemsg(buf);
  }

  rc = 0; /* not reached TODO under clean shutdown on signal */


 done:
  if (rc==-2) fprintf(stderr,"%s: %s\n", local, nn_strerror(errno));
  if(sock) nn_shutdown(sock,eid);
  kv_spoolwriter_free(sp);
  if (set) kv_set_free(set);
  utarray_free(output_keys);
  utarray_free(output_defaults);
  utarray_free(output_types);
  utstring_free(tmp);
  return rc;
}

