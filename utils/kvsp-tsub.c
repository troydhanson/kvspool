#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
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
#include "shr.h"
#include "utstring.h"
#include "kvspool_internal.h"
#include "kvsp-bconfig.h"

/* 
 * kvsp-tsub
 *
 * connect to TCP host
 * read binary frames
 * reverse to kv set
 * write to local spool
 *
 */

#define MAX_FRAME (1024*1024)
#define BUFSZ (MAX_FRAME * 10)
struct {
  char *prog;
  int verbose;
  int epoll_fd;     /* epoll descriptor */
  int signal_fd;    /* to receive signals */
  int client_fd;    /* connected tcp socket */
  char *host;       /* host to connect to */
  int port;         /* TCP port to connect to */
  char *cast;       /* cast config file name */
  char *spool;      /* spool file name */
  void *sp;         /* spool handle */
  void *set;        /* set handle */
  UT_string *tmp;   /* temp buffer */
  char buf[BUFSZ];  /* temp receive buffer */
  size_t bsz;       /* bytes ready in buf */
} cfg = {
  .host = "127.0.0.1",
  .epoll_fd = -1,
  .signal_fd = -1,
  .client_fd = -1,
};

/* signals that we'll accept via signalfd in epoll */
int sigs[] = {SIGHUP,SIGTERM,SIGINT,SIGQUIT,SIGALRM};

void usage() {
  fprintf(stderr,"usage: %s [options]\n", cfg.prog);
  fprintf(stderr,"required flags:\n"
                 "               -s <host>  (hostname)\n"
                 "               -p <port>  (port)\n"
                 "               -b <file>  (cast config)\n"
                 "               -d <spool> (spool dir)\n"
                 "other options:\n"
                 "               -v         (verbose)\n"
                 "               -h         (this help)\n"
                 "\n");
  exit(-1);
}

int connect_up(void) {
  int rc = -1, fd = -1;

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    fprintf(stderr,"socket: %s\n", strerror(errno));
    goto done;
  }

  struct hostent *h=gethostbyname(cfg.host);
  if (!h) {
    fprintf(stderr,"cannot resolve name: %s\n", hstrerror(h_errno));
    goto done;
  }
    
  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = ((struct in_addr*)h->h_addr)->s_addr;
  sin.sin_port = htons(cfg.port);

  if (connect(fd, (struct sockaddr*)&sin, sizeof(sin)) == -1) {
    fprintf(stderr,"connect: %s\n", strerror(errno));
    goto done;
  }

  cfg.client_fd = fd;
  rc = 0;

 done:
  if ((rc < 0) && (fd != -1)) close(fd);
  return rc;
}

int add_epoll(int events, int fd) {
  int rc;
  struct epoll_event ev;
  memset(&ev,0,sizeof(ev)); // placate valgrind
  ev.events = events;
  ev.data.fd= fd;
  if (cfg.verbose) fprintf(stderr,"adding fd %d to epoll\n", fd);
  rc = epoll_ctl(cfg.epoll_fd, EPOLL_CTL_ADD, fd, &ev);
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

/*
 * given a buffer of N frames 
 * with a possible partial final frame
 * decode them according to the cast config
 * saving the last frame prefix if partial
 */
int decode_frames(void) {
  char *c, *body, *eob;
  uint32_t blen;
  size_t remsz;
  int rc = -1;

  eob = cfg.buf + cfg.bsz;
  c = cfg.buf;
  while(1) {
    if (c + sizeof(uint32_t) > eob) break;
    memcpy(&blen, c, sizeof(uint32_t));
    if (blen > MAX_FRAME) goto done;
    body = c + sizeof(uint32_t);
    if (body + blen > eob) break;
    if (binary_to_frame(cfg.sp, cfg.set, body, blen, cfg.tmp) < 0) goto done;
    c += sizeof(uint32_t) + blen;
  }

  /* if buffer ends with partial frame, save it */
  if (c < eob) memmove(cfg.buf, c, eob - c);
  cfg.bsz = eob - c;

  rc = 0;

 done:
  if (rc < 0) fprintf(stderr, "frame parsing error\n");
  return rc;
}

/*
 * read from the publisher
 * each frame is prefixed with a uint32 length
 */
int handle_io(void) {
  int rc = -1;
  size_t avail;
  ssize_t nr;

  /* the buffer must have some free space because
   * any time we read data we process it right here,
   * leaving at most a tiny fragment of a partial
   * frame to prepend the next read */
  assert(cfg.bsz < BUFSZ);
  avail = BUFSZ - cfg.bsz;

  nr = read(cfg.client_fd, cfg.buf + cfg.bsz, avail);
  if (nr <= 0) {
    fprintf(stderr, "read: %s\n", nr ? strerror(errno) : "eof");
    goto done;
  }

  cfg.bsz += nr;
  if (decode_frames() < 0) goto done;

  rc = 0;

 done:
  return rc;
}

int main(int argc, char *argv[]) {
  int opt, rc=-1, n, ec;
  struct epoll_event ev;
  cfg.prog = argv[0];
  char unit, *c, buf[100];
  struct shr_stat stat;
  ssize_t nr;

  utstring_new(cfg.tmp);
  cfg.set = kv_set_new();

  utarray_new(output_keys, &ut_str_icd);
  utarray_new(output_defaults, &ut_str_icd);
  utarray_new(output_types,&ut_int_icd);

  while ( (opt = getopt(argc,argv,"vhs:p:d:b:")) > 0) {
    switch(opt) {
      case 'v': cfg.verbose++; break;
      case 'h': default: usage(); break;
      case 's': cfg.host = strdup(optarg); break;
      case 'p': cfg.port = atoi(optarg); break;
      case 'd': cfg.spool = strdup(optarg); break;
      case 'b': cfg.cast = strdup(optarg); break;
    }
  }

  if (cfg.spool == NULL) usage();
  if (cfg.cast == NULL) usage();
  if (cfg.host == NULL) usage();
  if (cfg.port == 0) usage();

  if (parse_config(cfg.cast) < 0) goto done;
  cfg.sp = kv_spoolwriter_new(cfg.spool);
  if (cfg.sp == NULL) goto done;

  if (connect_up() < 0) goto done;
  
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

  /* add descriptors of interest */
  if (add_epoll(EPOLLIN, cfg.signal_fd)) goto done;
  if (add_epoll(EPOLLIN, cfg.client_fd)) goto done;

  alarm(1);

  while (1) {
    ec = epoll_wait(cfg.epoll_fd, &ev, 1, -1);
    if (ec < 0) { 
      fprintf(stderr, "epoll: %s\n", strerror(errno));
      goto done;
    }

    if (ec == 0)                          { assert(0); goto done; }
    else if (ev.data.fd == cfg.signal_fd) { if (handle_signal()  < 0) goto done; }
    else if (ev.data.fd == cfg.client_fd) { if (handle_io() < 0) goto done; }
    else                                  { assert(0); goto done; }
  }
  
  rc = 0;
 
 done:
  utarray_free(output_keys);
  utarray_free(output_defaults);
  utarray_free(output_types);
  utstring_free(cfg.tmp);
  if (cfg.sp) kv_spoolwriter_free(cfg.sp);
  if (cfg.set) kv_set_free(cfg.set);
  if (cfg.signal_fd != -1) close(cfg.signal_fd);
  if (cfg.epoll_fd != -1) close(cfg.epoll_fd);
  if (cfg.client_fd != -1) close(cfg.client_fd);
  return 0;
}
