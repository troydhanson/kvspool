/* this program listens on a TCP port. when a client connects, it receives the
 * binary packed spool frames from the input spool. RR to multiple clients */
#define _GNU_SOURCE
#include <errno.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "utarray.h"
#include "utstring.h"
#include "kvspool.h"
#include "kvsp-bconfig.h"

struct {
  int verbose;
  char *prog;
  enum {fan, round_robin} mode;
  /* spool stuff */
  char *dir;
  void *sp;
  void *set;
  char *config_file; // cast config
  /* */
  int listener_port;
  int listener_fd;
  int signal_fd;
  int epoll_fd;
  int mb_per_client;
  UT_array *clients;
  UT_array *outbufs;
  int rr_idx;
  UT_string *s; // scratch
} cfg = {
  .mb_per_client=1,
  .mode=fan,
};

void usage() {
  fprintf(stderr,"usage: %s [-v] -p <port>  (tcp port to listen on - packet stream)\n"
                 "               -m <mb>    (megabytes to buffer to each client)\n"
                 "               -b <conf>  (binary cast config file)\n"
                 "               -d <spool> (spool to read)\n"
                 "               -r         (round robin mode, [def: fan mode])\n"
                 "\n", cfg.prog); 
  exit(-1);
}

/* signals that we'll accept via signalfd in epoll */
int sigs[] = {SIGHUP,SIGTERM,SIGINT,SIGQUIT,SIGALRM};

/* clean up the client output buffers and slots in fd/buf arrays */
void discard_client_buffers(int pos) {
  UT_string **s = (UT_string**)utarray_eltptr(cfg.outbufs,pos);
  utstring_free(*s);                // deep free string 
  utarray_erase(cfg.outbufs,pos,1); // erase string pointer
  utarray_erase(cfg.clients,pos,1); // erase client descriptor
}

void mark_writable() {
  /* mark writability-interest for any clients with pending output */
  int *fd=NULL;
  UT_string **s=NULL;
  while ( (fd=(int*)utarray_next(cfg.clients,fd))) {
    s=(UT_string**)utarray_next(cfg.outbufs,s); assert(s);
    if (utstring_len(*s)) mod_epoll(EPOLLIN|EPOLLOUT, *fd);
  }
}

/* clients whose out-buffers exceed thresh. get closed */
void cull_excess() {
  int *fd=NULL,pos;
  UT_string **s=NULL;
  while ( (fd=(int*)utarray_prev(cfg.clients,fd))) {
    s=(UT_string**)utarray_prev(cfg.outbufs,s); assert(s);
    if (utstring_len(*s) < cfg.mb_per_client*1024*1024) continue;

    /* this client output buffer is too big. close the client. */
    fprintf(stderr,"closing client@buffer max %d/%d mb\n",
      utstring_len(*s)/(1024*1024), cfg.mb_per_client);
    close(*fd); /* also deletes any epoll */
    pos = utarray_eltidx(cfg.clients,fd);
    discard_client_buffers(pos); 
  }

}

int set_to_binary(void *set, UT_string *bin) {
  uint32_t l, u, a,b,c,d, abcd;
  uint16_t s;
  uint8_t g;
  double h;
  utstring_clear(bin);
  l=0; utstring_bincpy(bin,&l,sizeof(l)); // placeholder for size prefix
  int rc=-1,i=0,*t;
  kv_t *kv, kvdef;
  char **k=NULL,**def;
  while( (k=(char**)utarray_next(output_keys,k))) {
    kv = kv_get(set,*k);
    t = (int*)utarray_eltptr(output_types,i); assert(t);
    def = (char**)utarray_eltptr(output_defaults,i); assert(def);
    if (kv==NULL) { /* no such key */
      kv=&kvdef;
      if (*def) {kv->val=*def; kv->vlen=strlen(*def);} /* default */
      else if (*t == str) {kv->val=NULL; kv->vlen=0;}  /* zero len string */
      else {
        fprintf(stderr,"required key %s not present in spool frame\n", *k);
        goto done;
      }
    }
    switch(*t) {
      case d64: h=atof(kv->val); utstring_bincpy(bin,&h,sizeof(h)); break;
      case i8:  g=atoi(kv->val); utstring_bincpy(bin,&g,sizeof(g)); break;
      case i16: s=atoi(kv->val); utstring_bincpy(bin,&s,sizeof(s)); break;
      case i32: u=atoi(kv->val); utstring_bincpy(bin,&u,sizeof(u)); break;
      case str: 
        l=kv->vlen; utstring_bincpy(bin,&l,sizeof(l)); /* length prefix */
        utstring_bincpy(bin,kv->val,kv->vlen);         /* string itself */
        break;
      case ipv4: 
        if ((sscanf(kv->val,"%u.%u.%u.%u",&a,&b,&c,&d) != 4) ||
           (a > 255 || b > 255 || c > 255 || d > 255)) {
          fprintf(stderr,"invalid IP for key %s: %s\n",*k,kv->val);
          goto done;
        }
        abcd = (a << 24) | (b << 16) | (c << 8) | d;
        abcd = htonl(abcd);
        utstring_bincpy(bin,&abcd,sizeof(abcd));
        break;
      default: assert(0); break;
    }
    i++;
  }
  uint32_t len = utstring_len(bin); len -= sizeof(len); // length does not include itself
  char *length_prefix = utstring_body(bin);
  memcpy(length_prefix, &len, sizeof(len));

  rc = 0;

 done:
  return rc;
}

void append_to_client_buf(UT_string *f) {
  UT_string **s;
  size_t l;
  char *b;

  b = utstring_body(f);
  l = utstring_len(f);

  if (cfg.mode == fan) {  // send to ALL clients
    s=NULL;
    while ( (s=(UT_string**)utarray_next(cfg.outbufs,s))) {
      utstring_bincpy(*s,b,l);
    }
  } else {                // send to ONE client (cfg.mode == round_robin)
    int nb = utarray_len(cfg.outbufs);
    int i = (cfg.rr_idx++) % nb;
    UT_string **s = (UT_string**)utarray_eltptr(cfg.outbufs,i);
    utstring_bincpy(*s,b,l);
  }
}

int check_spools() {
  if (utstring_len(cfg.outbufs) == 0) return 0; // no clients connected
  size_t n=0;
  int rc=-1;

  /* gather as much as we can from the spools, until it blocks or
   * we gather a threshhold of total bytes. the threshold is somewhat
   * arbitrary, here we overload cfg.mb_per_client as the threshold. */
  while (kv_spool_read(cfg.sp,cfg.set,0) > 0) {
    if (set_to_binary(cfg.set, cfg.s)) goto done;
    append_to_client_buf(cfg.s);
    n += utstring_len(cfg.s);
    if (n > cfg.mb_per_client*(1024*1024)) break;
  }

 rc = 0;

 done:
  return rc;
}

int periodic_work() {
  int rc = -1;

  cull_excess();
  if (check_spools()) goto done;
  mark_writable();
  rc = 0;

 done:
  return rc;
}

int setup_client_listener() {
  int rc = -1;

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    fprintf(stderr,"socket: %s\n", strerror(errno));
    goto done;
  }
  int one=1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = htonl(INADDR_ANY);
  sin.sin_port = htons(cfg.listener_port);

  if (bind(fd, (struct sockaddr*)&sin, sizeof(sin)) == -1) {
    fprintf(stderr,"bind: %s\n", strerror(errno));
    goto done;
  }

  if (listen(fd,1) == -1) {
    fprintf(stderr,"listen: %s\n", strerror(errno));
    goto done;
  }

  cfg.listener_fd = fd;
  rc=0;

 done:
  if ((rc < 0) && (fd != -1)) close(fd);
  return rc;
}

/* flush as much pending output to the client as it can handle. */
void feed_client(int ready_fd, int events) {
  int *fd=NULL, rc, pos, rv;
  char *buf, tmp[100];
  size_t len;
  UT_string **s=NULL;

  /* find the fd in our list */
  while ( (fd=(int*)utarray_next(cfg.clients,fd))) {
    s=(UT_string**)utarray_next(cfg.outbufs,s); assert(s);
    pos = utarray_eltidx(cfg.clients, fd);
    if (ready_fd == *fd) break;
  }
  assert(fd);

  if (cfg.verbose > 1) {
    fprintf(stderr, "pollout:%c pollin: %c\n", (events & EPOLLOUT)?'1':'0',
                                               (events & EPOLLIN) ?'1':'0');
  }

  /* before we write to the client, drain any input or closure */
  rv = recv(*fd, tmp, sizeof(tmp), MSG_DONTWAIT);
  if (rv == 0) {
    fprintf(stderr,"client closed (eof)\n");
    close(*fd); /* deletes epoll instances on *fd too */
    discard_client_buffers(pos);
    return;
  }

  if ((events & EPOLLOUT) == 0) return;

  /* send the pending buffer to the client */
  buf = utstring_body(*s);
  len = utstring_len(*s); assert(len);
  rc = send(*fd, buf, len, MSG_DONTWAIT);
  if (cfg.verbose) fprintf(stderr,"sent %d/%d bytes\n", rc, (int)len);

  /* test for client closure or error. */
  if (rc < 0) {
    if ((errno == EWOULDBLOCK) || (errno == EAGAIN)) return;
    fprintf(stderr,"client closed (%s)\n", strerror(errno));
    close(*fd); /* deletes all epoll instances on *fd too */
    discard_client_buffers(pos);
    return;
  }

  /* shift the output buffer; we wrote rc bytes TODO noshift */
  if (rc < len) {
    memmove((*s)->d, (*s)->d + rc, len-rc);
    (*s)->i -= rc;
  } else {
    utstring_clear(*s);     // buffer emptied
    mod_epoll(EPOLLIN,*fd); // remove EPOLLOUT 
  }
}


int new_epoll(int events, int fd) {
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

int mod_epoll(int events, int fd) {
  int rc;
  struct epoll_event ev;
  memset(&ev,0,sizeof(ev)); // placate valgrind
  ev.events = events;
  ev.data.fd= fd;
  if (cfg.verbose) fprintf(stderr,"modding fd %d epoll\n", fd);
  rc = epoll_ctl(cfg.epoll_fd, EPOLL_CTL_MOD, fd, &ev);
  if (rc == -1) {
    fprintf(stderr,"epoll_ctl: %s\n", strerror(errno));
  }
  return rc;
}

int handle_signal() {
  int rc=-1;
  struct signalfd_siginfo info;
  
  if (read(cfg.signal_fd, &info, sizeof(info)) != sizeof(info)) {
    fprintf(stderr,"failed to read signal fd buffer\n");
    goto done;
  }

  switch(info.ssi_signo) {
    case SIGALRM: 
      if (periodic_work()) goto done;
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

int accept_client() {
  int rc = -1;

  struct sockaddr_in cin;
  socklen_t cin_sz = sizeof(cin);
  int fd = accept(cfg.listener_fd,(struct sockaddr*)&cin, &cin_sz);
  if (fd == -1) {
    fprintf(stderr,"accept: %s\n", strerror(errno));
    goto done;
  }
  if (sizeof(cin)==cin_sz) fprintf(stderr,"connection from %s:%d\n", 
    inet_ntoa(cin.sin_addr), (int)ntohs(cin.sin_port));
  utarray_push_back(cfg.clients,&fd);
  /* set up client output buffer. reserve space for full buffer */
  UT_string *s; utstring_new(s); utstring_reserve(s,cfg.mb_per_client*1024*1024); 
  utarray_push_back(cfg.outbufs,&s);
  new_epoll(EPOLLIN, fd);

  rc=0;

 done:
  return rc;
}

int main(int argc, char *argv[]) {
  int opt, rc, n, *fd;
  cfg.prog = argv[0];
  utarray_new(cfg.clients,&ut_int_icd);
  utarray_new(cfg.outbufs,&ut_ptr_icd);
  cfg.set = kv_set_new();
  struct epoll_event ev;
  UT_string **s;

  utstring_new(cfg.s);
  utarray_new(output_keys, &ut_str_icd);
  utarray_new(output_defaults, &ut_str_icd);
  utarray_new(output_types,&ut_int_icd);

  while ( (opt=getopt(argc,argv,"vb:p:m:d:rh")) != -1) {
    switch(opt) {
      case 'v': cfg.verbose++; break;
      case 'p': cfg.listener_port=atoi(optarg); break; 
      case 'm': cfg.mb_per_client=atoi(optarg); break; 
      case 'd': cfg.dir=strdup(optarg); break; 
      case 'r': cfg.mode=round_robin; break; 
      case 'b': cfg.config_file=strdup(optarg); break;
      case 'h': default: usage(); break;
    }
  }
  if (cfg.listener_port==0) usage();
  if (setup_client_listener()) goto done;
  if (cfg.config_file==NULL) goto done;
  if (parse_config(cfg.config_file) < 0) goto done;
  if ( !(cfg.sp = kv_spoolreader_new(cfg.dir))) goto done;

  /* block all signals. we take signals synchronously via signalfd */
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
  if (new_epoll(EPOLLIN, cfg.listener_fd)) goto done; // new client connections
  if (new_epoll(EPOLLIN, cfg.signal_fd))   goto done; // signal socket

  alarm(1);
  while (epoll_wait(cfg.epoll_fd, &ev, 1, -1) > 0) {

    if (cfg.verbose > 1)  fprintf(stderr,"epoll reports fd %d\n", ev.data.fd);
    if      (ev.data.fd == cfg.signal_fd)   { if (handle_signal() < 0) goto done; }
    else if (ev.data.fd == cfg.listener_fd) { if (accept_client() < 0) goto done; }
    else    feed_client(ev.data.fd, ev.events);
  }

done:
  /* free the clients: close and deep free their buffers */
  fd=NULL; s=NULL;
  while ( (fd=(int*)utarray_prev(cfg.clients,fd))) {
    s=(UT_string**)utarray_prev(cfg.outbufs,s);
    close(*fd);
    utstring_free(*s);
  }
  utarray_free(cfg.clients);
  utarray_free(cfg.outbufs);
  utarray_free(output_keys);
  utarray_free(output_defaults);
  utarray_free(output_types);
  utstring_free(cfg.s);
  if (cfg.listener_fd) close(cfg.listener_fd);
  if (cfg.signal_fd) close(cfg.signal_fd);
  if (cfg.sp) kv_spoolreader_free(cfg.sp);
  if (cfg.set) kv_set_free(cfg.set);
  return 0;
}
