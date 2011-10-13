#include <stdio.h>
#include <signal.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>
#include <time.h>
#include <sys/prctl.h>
#include "kvspool_internal.h"
#include "utarray.h"
#include "tpl.h"

#define SPOOL_EXPORT_DEFAULT_PORT 3339
#define STATS_INTERVAL 1000000

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] [-l <ip>] [-p <port>] [-b base] spool\n", prog);
  fprintf(stderr, "          -l    listen on given IP (def: all IP's)\n");
  fprintf(stderr, "          -p    listen on given port (def:%d)\n", 
                                     (int)SPOOL_EXPORT_DEFAULT_PORT);
  fprintf(stderr, "          -b    only export given base (def: all)\n");
  exit(-1);
}

char proctitle[16];
void update_stats(time_t *start, long *count) {
  time_t now = time(NULL);
  long elapsed = now - *start; 
  if (elapsed == 0) return;
  unsigned kfps = *count / (1024 * elapsed);
  snprintf(proctitle,sizeof(proctitle),"export %u kfps", kfps);
  prctl(PR_SET_NAME,(unsigned long)proctitle,0,0,0);
  fprintf(stderr, "export %u kfps\n", kfps);
  *start = now;
  *count = 0;
}
 
int setup_listener(char *ip_str, int port) {
  /**********************************************************
   * create an IPv4/TCP socket, not yet bound to any address
   *********************************************************/
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    printf("socket: %s\n", strerror(errno));
    return -1;
  }

  /**********************************************************
   * internet socket address structure: our address and port
   *********************************************************/
  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = htonl(INADDR_ANY); /* TODO use ip_str */
  sin.sin_port = htons(port);

  /**********************************************************
   * bind socket to address and port we'd like to receive on
   *********************************************************/
  int optval = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
  if (bind(fd, (struct sockaddr*)&sin, sizeof(sin)) == -1) {
    printf("bind: %s\n", strerror(errno));
    close(fd);
    return -1;
  }

  /**********************************************************
   * put socket into listening state 
   *********************************************************/
  if (listen(fd,1) == -1) {
    printf("listen: %s\n", strerror(errno));
    close(fd);
    return -1;
  }
  return fd;
}

char *last_base=NULL;
int export_frame(void *img, size_t sz, char *base, int fd) {
  int rc;
  if (last_base != base) {  /* yes just a pointer compare */
    if (tpl_jot(TPL_FD, fd, "s", &base) == -1) return -1;
    last_base = base;
  }

  char *buf = (char*)img;
  while (sz) {
    rc = write(fd, buf, sz);
    if (rc == -1 && (errno == EAGAIN || errno == EINTR)) continue;
    if (rc == -1) return -1;
    sz -= rc; buf += rc;
  }
  free(img);
  return 0;
}

int main(int argc, char * argv[]) {
  int opt,verbose=0,client_fd=-1,listen_fd=-1,tryagain=0;
  void *set;
  char *ip_str=NULL;
  int port=SPOOL_EXPORT_DEFAULT_PORT;
  long count=0;
  /* exported spool */
  char *dir=NULL;
  char *base=NULL;
  void *sp;
  time_t start = time(NULL);

  signal(SIGPIPE,SIG_IGN);

  set = kv_set_new();
 
  while ( (opt = getopt(argc, argv, "v+l:p:b:")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'l': ip_str = strdup(optarg); break;
      case 'p': port = atoi(optarg); break;
      case 'b': base = strdup(optarg); break;
      default: usage(argv[0]); break;
    }
  }
  if (optind < argc) dir = argv[optind++];
  if (dir==NULL) usage(argv[0]);
  sp = kv_spoolreader_new(dir,base);
  if (sp == NULL) {
    fprintf(stderr, "failed to open input spool %s\n", dir);
    goto done;
  }

  listen_fd = setup_listener(ip_str,port);
  if (listen_fd == -1) goto done;

  while (1) {
    struct sockaddr_in cin;
    socklen_t cin_sz = sizeof(cin);
    client_fd = accept(listen_fd, (struct sockaddr*)&cin, &cin_sz);
    if (client_fd == -1) {
      fprintf(stderr,"accept: %s\n",strerror(errno));
      continue;
    }
    if (sizeof(cin)==cin_sz) printf("connection from %s:%d\n",
      inet_ntoa(cin.sin_addr), (int)ntohs(cin.sin_port));

    last_base=NULL;
    while (tryagain || (kv_spool_read(sp,set,1) == 1)) {
      kvset_t *_set = (kvset_t*)set;
      if (export_frame(_set->img,_set->len,_set->base,client_fd) == -1) {
        tryagain=1;
        close(client_fd); client_fd=-1;
        break;
      }
      tryagain=0;
      if ((++count % STATS_INTERVAL) == 0) update_stats(&start,&count);
    }
  }

 done:
  if (listen_fd != -1) close(listen_fd);
  if (client_fd != -1) close(client_fd);
  kv_set_free(set);
  kv_spoolreader_free(sp);
  return 0;
}

