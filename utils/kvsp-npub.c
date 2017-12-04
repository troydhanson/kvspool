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
#include <nanomsg/nn.h>
#include <nanomsg/pipeline.h>
#include <arpa/inet.h>
#include "utarray.h"
#include "utstring.h"
#include "kvspool.h"
#include "kvsp-bconfig.h"

char *remotes_file;
char *config_file;
UT_string *tmp;
int sock,eid;
int verbose;
char *spool;
void *set;
void *sp;

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] -b <cast-config> -d spool [-f <remotes-file>] [<remote> ...]\n", prog);
  fprintf(stderr, "  <remote> is the nsub peer e.g. tcp://127.0.0.1:1234\n");
  exit(-1);
}

int read_lines(char *file, UT_array *lines) {
  char line[200];
  int rc = -1;
  char *c;
  FILE *f = fopen(file,"r");
  if (f==NULL) {
    fprintf(stderr,"fopen %s: %s\n", file, strerror(errno));
    goto done;
  }
  while (fgets(line,sizeof(line),f) != NULL) {
    for(c=line; (c < line+sizeof(line)) && (*c != '\0'); c++) {
     if (*c == '\n') *c='\0';
     if (*c == ' ') *c='\0';
    }
    c = line;
    if (strlen(c) == 0) continue;
    utarray_push_back(lines,&c);
  }
  rc  = 0;

 done:
  if (f) fclose(f);
  return rc;
}

int main(int argc, char *argv[]) {
  int opt,rc=-1;
  size_t len;
  void *buf;
  UT_array *endpoints;
  char **endpoint;

  set = kv_set_new();
  utarray_new(output_keys, &ut_str_icd);
  utarray_new(output_defaults, &ut_str_icd);
  utarray_new(output_types,&ut_int_icd);
  utstring_new(tmp);
  utarray_new(endpoints,&ut_str_icd);

  while ( (opt = getopt(argc, argv, "v+d:b:f:h")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'd': spool=strdup(optarg); break;
      case 'b': config_file=strdup(optarg); break;
      case 'f': remotes_file=strdup(optarg); break;
      case 'h': default: usage(argv[0]); break;
    }
  }
  if (remotes_file) if (read_lines(remotes_file,endpoints)) goto done;
  while (optind < argc) utarray_push_back(endpoints,&argv[optind++]);
  if (utarray_len(endpoints) == 0) usage(argv[0]);
  if (spool == NULL) usage(argv[0]);
  if (parse_config(config_file) < 0) goto done;
  if ( !(sp = kv_spoolreader_new(spool))) goto done;
  rc = -2;

  if ( (sock = nn_socket(AF_SP, NN_PUSH)) < 0) goto done;
  endpoint=NULL;
  while ( (endpoint=(char**)utarray_next(endpoints,endpoint))) {
    if (verbose) fprintf(stderr,"connecting to %s\n", *endpoint);
    if ( (eid = nn_connect(sock, *endpoint)) < 0) goto done;
  }

  while (kv_spool_read(sp,set,1) > 0) { /* read til interrupted by signal */
    if (set_to_binary(set,tmp) < 0) goto done;
    buf = utstring_body(tmp);
    len = utstring_len(tmp);
    /* skip length preamble */
    if (len <= sizeof(uint32_t)) goto done;
    len -= sizeof(uint32_t);
    buf += sizeof(uint32_t);

    rc = nn_send(sock, buf, len, 0);
    if (rc == -1) goto done;
  }

  rc = 0;

 done:
  if (rc==-2) fprintf(stderr,"nano: %s\n", nn_strerror(errno));
  if (sock) nn_close(sock);
  if (sp) kv_spoolreader_free(sp);
  kv_set_free(set);
  utarray_free(output_keys);
  utarray_free(output_defaults);
  utarray_free(output_types);
  utstring_free(tmp);
  utarray_free(endpoints);

  return 0;
}

