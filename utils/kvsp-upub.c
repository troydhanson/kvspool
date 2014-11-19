/* this utility sends spool frames out as UDP packets with JSON payloads */
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
#include <arpa/inet.h>
#include <netdb.h>
#include "utarray.h"
#include "utstring.h"
#include "kvspool.h"
#include "kvsp-bconfig.h"
#include <jansson.h>

int verbose;
int port=5139; // arbitrary
char *spool;
UT_string *buf;
UT_array *fds;

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] -d spool -p <port> <remoteIP> ... \n", prog);
  exit(-1);
}

void setup_udp(char *host) {
  if (verbose) fprintf(stderr,"connecting to %s\n",host);
  int fd;
  in_addr_t addr;
  struct hostent *h = gethostbyname(host);
  if (!h) {fprintf(stderr,"%s\n",hstrerror(h_errno)); exit(-1);}
  addr = ((struct in_addr*)h->h_addr)->s_addr;

  /**********************************************************
   * create an IPv4/UDP socket, not yet bound to any address
   *********************************************************/
  fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd == -1) {
    fprintf(stderr,"socket: %s\n", strerror(errno));
    exit(-1);
  }

  /**********************************************************
   * internet socket address structure, for the remote side
   *********************************************************/
  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = addr;
  sin.sin_port = htons(port);

  if (sin.sin_addr.s_addr == INADDR_NONE) {
    fprintf(stderr,"invalid remote IP %s\n", host);
    exit(-1);
  }

  /**********************************************************
   * UDP is connectionless; connect only sets dest for writes
   *********************************************************/
  if (connect(fd, (struct sockaddr*)&sin, sizeof(sin)) == -1) {
    fprintf(stderr,"connect: %s\n", strerror(errno));
    exit(-1);
  }
  utarray_push_back(fds,&fd);
}

int main(int argc, char *argv[]) {
  void *sp=NULL;
  void *set=NULL;
  int c, opt,rc=-1;
  size_t sz; void *b;
  json_t *o = NULL;
  char *config_file;
  set = kv_set_new();
  utarray_new(fds,&ut_int_icd);
  utstring_new(buf);
  o = json_object();

  signal(SIGPIPE,SIG_IGN);

  while ( (opt = getopt(argc, argv, "v+d:p:")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'd': spool=strdup(optarg); break;
      case 'p': port=atoi(optarg); break;
      default: usage(argv[0]); break;
    }
  }
  if (spool == NULL) usage(argv[0]);
  while(optind < argc) setup_udp(argv[optind++]);

  sp = kv_spoolreader_new(spool);
  if (!sp) goto done;

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
    int *fd=NULL;
    while ( (fd=(int*)utarray_next(fds,fd))) {
      if (write(*fd,json,len) == -1) {
        fprintf(stderr,"write: %s\n",strerror(errno));
        goto done;
      }
    }
    free(json);
  }


  // close all the fds.
  // TODO

  rc = 0;

 done:
  if (sp) kv_spoolreader_free(sp);
  kv_set_free(set);
  utarray_free(fds);
  utstring_free(buf);
  if(o) json_decref(o);

  return 0;
}
