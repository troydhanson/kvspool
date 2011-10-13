#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "../kvspool_internal.h"
#include "utarray.h"
#include "tpl.h"

#define SPOOL_EXPORT_DEFAULT_PORT 3339

typedef struct {
  char *base;
  void *sp;
} ospool_base_t;

/* plumbing for ospool_base_t array */
void ospool_base_ini(void *_ospb) {
  ospool_base_t *ospb = (ospool_base_t*)_ospb;
  ospb->base=NULL;
  ospb->sp=NULL;
}
void ospool_base_cpy(void *_dst, const void *_src) {
  assert(0); /* enforce that this function is not used */
  /* note that copy is not used here because dst->sp has
   * no ref counting support; but our 'fin' frees it. */
                        
}
void ospool_base_fin(void *_ospb) {
  ospool_base_t *ospb = (ospool_base_t*)_ospb;
  if (ospb->base) free(ospb->base);
  if (ospb->sp) kv_spoolwriter_free(ospb->sp);
}

UT_icd ospool_base_icd = {sizeof(ospool_base_t),
                          ospool_base_ini,
                          ospool_base_cpy,
                          ospool_base_fin};
 
void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] -s <ip> [-p <port>] spool\n", prog);
  fprintf(stderr, "          -s    connect to given IP\n");
  fprintf(stderr, "          -p    listen on given port (def:%d)\n", 
                                     (int)SPOOL_EXPORT_DEFAULT_PORT);
  exit(-1);
}

int connect_to_exporter(char *ip_str, int port) {
  /**********************************************************
   * create an IPv4/TCP socket, not yet bound to any address
   *********************************************************/
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    fprintf(stderr,"socket: %s\n", strerror(errno));
    return -1;
  }
  /**********************************************************
   * internet socket address structure, for the remote side
   *********************************************************/
  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = inet_addr(ip_str);
  sin.sin_port = htons(port);

  if (sin.sin_addr.s_addr == INADDR_NONE) {
    fprintf(stderr,"invalid remote IP %s\n", ip_str);
    close(fd);
    return -1;
  }

  /**********************************************************
   * Perform the 3 way handshake, (c)syn, (s)ack/syn, c(ack)
   *********************************************************/
  if (connect(fd, (struct sockaddr*)&sin, sizeof(sin)) == -1) {
    fprintf(stderr,"connect: %s\n", strerror(errno));
    close(fd);
    return -1;
  }

  return fd;
}
 
int main(int argc, char * argv[]) {
  int rc, opt,verbose=0,server_fd=-1;
  void *set,*img;
  size_t sz;
  char *ip_str=NULL;
  int port=SPOOL_EXPORT_DEFAULT_PORT;
  char *dir=NULL,*base=NULL;

  UT_array *ospoolv;
  utarray_new(ospoolv, &ospool_base_icd);

  set = kv_set_new();
 
  while ( (opt = getopt(argc, argv, "v+s:p:")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 's': ip_str = strdup(optarg); break;
      case 'p': port = atoi(optarg); break;
      default: usage(argv[0]); break;
    }
  }
  if (optind < argc) dir = argv[optind++];
  if (dir==NULL) usage(argv[0]);

  /* connect to the listening exporter */
  server_fd = connect_to_exporter(ip_str,port);
  if (server_fd == -1) {
    fprintf(stderr,"can't connect to server\n");
    goto done;
  }


  while(1) {
    if (tpl_gather(TPL_GATHER_BLOCKING, server_fd, &img, &sz) <= 0) {
      fprintf(stderr,"closing connection to server\n");
      goto done;
    }

    /* is it a base specification? this is a no-op if not */
    char *_base;
    char *fmt = tpl_peek(TPL_MEM|TPL_DATAPEEK, img, sz, "s", &_base);
    if (fmt) { 
      if (base) free(base);
      base = _base;
      free(fmt);
      free(img);
      continue;
    }

    assert(base);
    /* find spool writer for this base and write out the frame */
    ospool_base_t *o=NULL;
    while( (o=(ospool_base_t*)utarray_next(ospoolv,o))) {
      if (strcmp(o->base,base)) continue;
      break;
    }
    if (o == NULL) { /* don't have one already; make one */
      utarray_extend_back(ospoolv);
      o = (ospool_base_t*)utarray_back(ospoolv);
      o->base = strdup(base);
      o->sp = kv_spoolwriter_new(dir,base);
      if (!o->sp) {
        fprintf(stderr,"failed to open output spool %s base %s\n",dir,base);
        goto done;
      }
    }
    kv_write_raw_frame(o->sp,img,sz);
    free(img);
  }

 done:
  if (server_fd != -1) close(server_fd);
  utarray_free(ospoolv);
  kv_set_free(set);
  return 0;
}

