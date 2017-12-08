#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <sys/signalfd.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include "kvspool_internal.h"
#include "kvsp-bconfig.h"
#include "ringbuf.h"

/* 
 * publish spool over TCP in binary
 */

#define BATCH_FRAMES 10000
#define OUTPUT_BUFSZ (10 * 1024 * 1024)

struct {
  char *prog;
  enum {mode_pub } mode;
  int verbose;
  int epoll_fd;     /* epoll descriptor */
  uint32_t events;  /* epoll event status */
  int signal_fd;    /* to receive signals */
  int listen_fd;    /* listening tcp socket */
  int client_fd;    /* connected tcp socket */
  in_addr_t addr;   /* IP address to listen on */
  int port;         /* TCP port to listen on */
  char *spool;      /* spool file name */
  void *sp;         /* spool handle */
  int spool_fd;     /* spool descriptor */
  char *cast;       /* cast file name */
  void *set;        /* kvspool set */
  UT_string *tmp;   /* scratch area */
  ringbuf *rb;      /* pending output */
} cfg = {
  .addr = INADDR_ANY, /* by default, listen on all local IP's */
  .port = 1919,       /* arbitrary */
  .epoll_fd = -1,
  .signal_fd = -1,
  .listen_fd = -1,
  .spool_fd = -1,
  .client_fd = -1,
};

/* signals that we'll accept via signalfd in epoll */
int sigs[] = {SIGHUP,SIGTERM,SIGINT,SIGQUIT,SIGALRM};

void usage() {
  fprintf(stderr,"usage: %s [options] \n", cfg.prog);
  fprintf(stderr,"options:\n"
                 "               -p <port>  (TCP port to listen on)\n"
                 "               -d <spool> (spool directory to read)\n"
                 "               -b <cast>  (cast config file)\n"
                 "               -v         (verbose)\n"
                 "               -h         (this help)\n"
                 "\n");
  exit(-1);
}

int add_epoll(int events, int fd) {
  int rc;
  struct epoll_event ev;
  memset(&ev,0,sizeof(ev)); // placate valgrind
  ev.events = events;
  ev.data.fd= fd;
  if (cfg.verbose) fprintf(stderr,"adding fd %d events %d\n", fd, events);
  rc = epoll_ctl(cfg.epoll_fd, EPOLL_CTL_ADD, fd, &ev);
  if (rc == -1) {
    fprintf(stderr,"epoll_ctl: %s\n", strerror(errno));
  }
  return rc;
}

int mod_epoll(int events, int fd) {
  int rc;
  struct epoll_event ev;
  memset(&ev,0,sizeof(ev)); // placate valgrind
  ev.events = events;
  ev.data.fd= fd;
  if (cfg.verbose) fprintf(stderr,"modding fd %d events %d\n", fd, events);
  rc = epoll_ctl(cfg.epoll_fd, EPOLL_CTL_MOD, fd, &ev);
  if (rc == -1) {
    fprintf(stderr,"epoll_ctl: %s\n", strerror(errno));
  }
  return rc;
}

int del_epoll(int fd) {
  int rc;
  struct epoll_event ev;
  rc = epoll_ctl(cfg.epoll_fd, EPOLL_CTL_DEL, fd, &ev);
  if (rc == -1) {
    fprintf(stderr,"epoll_ctl: %s\n", strerror(errno));
  }
  return rc;
}

/* work we do at 1hz  */
int periodic_work(void) {
  int rc = -1;

  rc = 0;

 done:
  return rc;
}

int handle_signal(void) {
  int rc=-1;
  struct signalfd_siginfo info;
  
  if (read(cfg.signal_fd, &info, sizeof(info)) != sizeof(info)) {
    fprintf(stderr,"failed to read signal fd buffer\n");
    goto done;
  }

  switch(info.ssi_signo) {
    case SIGALRM: 
      if (periodic_work() < 0) goto done;
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

int setup_listener() {
  int rc = -1, one=1;

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    fprintf(stderr,"socket: %s\n", strerror(errno));
    goto done;
  }

  /**********************************************************
   * internet socket address structure: our address and port
   *********************************************************/
  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = cfg.addr;
  sin.sin_port = htons(cfg.port);

  /**********************************************************
   * bind socket to address and port 
   *********************************************************/
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  if (bind(fd, (struct sockaddr*)&sin, sizeof(sin)) == -1) {
    fprintf(stderr,"bind: %s\n", strerror(errno));
    goto done;
  }

  /**********************************************************
   * put socket into listening state
   *********************************************************/
  if (listen(fd,1) == -1) {
    fprintf(stderr,"listen: %s\n", strerror(errno));
    goto done;
  }

  if (add_epoll(EPOLLIN, fd)) goto done;
  cfg.listen_fd = fd;
  rc=0;

 done:
  if ((rc < 0) && (fd != -1)) close(fd);
  return rc;
}

/* accept a new client connection to the listening socket */
int accept_client() {
  int fd=-1, rc=-1;
  struct sockaddr_in in;
  socklen_t sz = sizeof(in);

  fd = accept(cfg.listen_fd,(struct sockaddr*)&in, &sz);
  if (fd == -1) {
    fprintf(stderr,"accept: %s\n", strerror(errno)); 
    goto done;
  }

  if (cfg.verbose && (sizeof(in)==sz)) {
    fprintf(stderr,"connection fd %d from %s:%d\n", fd,
    inet_ntoa(in.sin_addr), (int)ntohs(in.sin_port));
  }

  if (cfg.client_fd != -1) { /* already have a client? */
    fprintf(stderr,"refusing client\n");
    close(fd);
    rc = 0;
    goto done;
  }

  cfg.client_fd = fd;

  /* epoll on both the spool and the client */
  if (add_epoll(EPOLLIN, cfg.client_fd) < 0) goto done;
  mod_epoll(EPOLLIN, cfg.spool_fd);

  rc = 0;

 done:
  return rc;
}

int handle_spool(void) {
  int rc = -1, sc, i=0;
  char *buf;
  size_t len;
  int fl;

  while(i++ < BATCH_FRAMES) {

    /* suspend spool reading if output buffer < 10% free */
    if (ringbuf_get_freespace(cfg.rb) < (0.1 * OUTPUT_BUFSZ)) {
      mod_epoll(0, cfg.spool_fd);
      break;
    }

    sc = kv_spool_read(cfg.sp, cfg.set, 0);
    if (sc < 0) {
      fprintf(stderr, "kv_spool_read: error\n");
      goto done;
    }

    /* no new data in spool ? */
    if ( sc == 0 ) break;

    sc = set_to_binary(cfg.set, cfg.tmp);
    if (sc < 0) goto done;

    buf = utstring_body(cfg.tmp);
    len = utstring_len(cfg.tmp);
    sc = ringbuf_put(cfg.rb, buf, len);
    if (sc < 0) {
      /* unexpected; we checked it was 10% free */
      fprintf(stderr, "buffer exhausted\n");
      goto done;
    }
  }

  fl = EPOLLIN | (ringbuf_get_pending_size(cfg.rb) ? EPOLLOUT : 0);
  mod_epoll(fl, cfg.client_fd);
  rc = 0;

 done:
  return rc;
}

void close_client(void) {
  ringbuf_clear(cfg.rb);
  mod_epoll(0,cfg.spool_fd); /* ignore spool til new client */
  close(cfg.client_fd);      /* close removes client epoll */
  cfg.client_fd = -1;
  cfg.events = 0;
}

void drain_client(void) {
  char buf[1024];
  ssize_t nr;

  nr = read(cfg.client_fd, buf, sizeof(buf));
  if(nr > 0) { 
    if (cfg.verbose) fprintf(stderr,"client: %lu bytes\n", (long unsigned)nr);
    return;
  }

  /* disconnect or socket error are handled the same - close it */
  assert(nr <= 0);
  fprintf(stderr,"client: %s\n", nr ? strerror(errno) : "closed");
  close_client();
}

void send_client(void) {
  size_t nr, wr;
  char *buf;
  int fl;

  nr = ringbuf_get_next_chunk(cfg.rb, &buf);
  assert(nr > 0);

  wr = write(cfg.client_fd, buf, nr);
  if (wr < 0) {
    fprintf(stderr, "write: %s\n", strerror(errno));
    close_client();
    return;
  }

  ringbuf_mark_consumed(cfg.rb, wr);

  /* adjust epoll on client based on we have more output to send */
  fl = EPOLLIN | (ringbuf_get_pending_size(cfg.rb) ? EPOLLOUT : 0);
  mod_epoll(fl, cfg.client_fd);

  /* reinstate/retain spool reads if output buffer is > 10% free */
  if (ringbuf_get_freespace(cfg.rb) > (0.1 * OUTPUT_BUFSZ)) {
    mod_epoll(EPOLLIN, cfg.spool_fd);
  }
}

int handle_client() {
  int rc = -1;
  assert(cfg.client_fd != -1);

  if (cfg.events & EPOLLIN) drain_client();
  if (cfg.events & EPOLLOUT) send_client();

  rc = 0;
  return rc;
}

int main(int argc, char *argv[]) {
  int opt, rc=-1, n, ec;
  struct epoll_event ev;
  cfg.prog = argv[0];
  char unit, *c, buf[100];
  ssize_t nr;

  utarray_new(output_keys, &ut_str_icd);
  utarray_new(output_defaults, &ut_str_icd);
  utarray_new(output_types,&ut_int_icd);
  cfg.set = kv_set_new();
  utstring_new(cfg.tmp);
  cfg.rb = ringbuf_new(OUTPUT_BUFSZ);
  if (cfg.rb == NULL) goto done;

  while ( (opt = getopt(argc,argv,"vhp:d:b:")) > 0) {
    switch(opt) {
      case 'v': cfg.verbose++; break;
      case 'h': default: usage(); break;
      case 'p': cfg.port = atoi(optarg); break;
      case 'd': cfg.spool = strdup(optarg); break;
      case 'b': cfg.cast = strdup(optarg); break;
    }
  }

  if (cfg.spool == NULL) usage();
  if (cfg.cast == NULL) usage();
  
  if (parse_config(cfg.cast) < 0) goto done;
  cfg.sp = kv_spoolreader_new_nb(cfg.spool, &cfg.spool_fd);
  if (cfg.sp == NULL) goto done;

  /* block all signals. we accept signals via signal_fd */
  sigset_t all;
  sigfillset(&all);
  sigprocmask(SIG_SETMASK,&all,NULL);

  /* a few signals we'll accept via our signalfd */
  sigset_t sw;
  sigemptyset(&sw);
  for(n=0; n < sizeof(sigs)/sizeof(*sigs); n++) sigaddset(&sw, sigs[n]);

  /* create the signalfd for receiving signals */
  cfg.signal_fd = signalfd(-1, &sw, 0);
  if (cfg.signal_fd == -1) {
    fprintf(stderr,"signalfd: %s\n", strerror(errno));
    goto done;
  }

  /* set up the epoll instance */
  cfg.epoll_fd = epoll_create(1); 
  if (cfg.epoll_fd == -1) {
    fprintf(stderr,"epoll: %s\n", strerror(errno));
    goto done;
  }

  /* add descriptors of interest. idle spool til connect */
  if (add_epoll(EPOLLIN, cfg.signal_fd)) goto done;
  if (add_epoll(0, cfg.spool_fd) < 0) goto done;

  if (setup_listener() < 0) goto done;

  alarm(1);

  while (1) {
    ec = epoll_wait(cfg.epoll_fd, &ev, 1, -1);
    if (ec < 0) { 
      fprintf(stderr, "epoll: %s\n", strerror(errno));
      goto done;
    }

    cfg.events = ev.events;

    if (ec == 0)                          { assert(0); goto done; }
    else if (ev.data.fd == cfg.signal_fd) { if (handle_signal()  < 0) goto done; }
    else if (ev.data.fd == cfg.listen_fd) { if (accept_client() < 0) goto done; }
    else if (ev.data.fd == cfg.client_fd) { if (handle_client() < 0) goto done; }
    else if (ev.data.fd == cfg.spool_fd)  { if (handle_spool() < 0) goto done; }
    else                                  { assert(0); goto done; }
  }
  
  rc = 0;
 
 done:
  if (cfg.signal_fd != -1) close(cfg.signal_fd);
  if (cfg.epoll_fd != -1) close(cfg.epoll_fd);
  if (cfg.listen_fd != -1) close(cfg.listen_fd);
  if (cfg.client_fd != -1) close(cfg.client_fd);
  if (cfg.sp) kv_spoolreader_free(cfg.sp);
  kv_set_free(cfg.set);
  utstring_free(cfg.tmp);
  if (cfg.rb) ringbuf_free(cfg.rb);
  return 0;
}
