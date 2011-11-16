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
#include <zmq.h>
#include "utarray.h"
#include "kvspool_internal.h"

#if ZMQ_VERSION_MAJOR == 2
#define zmq_sendmsg zmq_send
#define zmq_hwm_t uint64_t
#else
#define zmq_hwm_t int
#endif
/******************************************************************************
 * kvsp-publish
 * This program monitors multiple spools using a pool of readers
 * Whenever a spool has a frame that's readable it is published to subscribers 
 *****************************************************************************/

typedef struct {
  pid_t pid;
  time_t start;
  char *dir;
  char *transport;
} worker_t;

#define SHORT_DELAY 10
const zmq_hwm_t hwm = 10000; /* high water mark: max messages pub will buffer */

worker_t *workers;
int wn=1; /* workers needed: 1 device + 1 publisher per spool */
int verbose;
int push_mode;
char *file;
char *pub_transport; /* client kvsp-sub's connect to us on this transport */
UT_array *dirs;

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] [-s] [-d dir [-d dir ...]] <path>\n", prog);
  fprintf(stderr, "  -s runs in push mode instead of lossy pub-sub\n");
  fprintf(stderr, "  <path> is a 0mq path e.g. tcp://localhost:1234\n");
  exit(-1);
}

void configure_worker(int n) {
   worker_t *w = &workers[n];
   char transport[100];
   snprintf(transport,sizeof(transport),"ipc://kvsp-pub-%u:%u",(int)getpid(),n);
   w->transport = strdup(transport);
   w->dir = strdup(*(char**)utarray_eltptr(dirs,n-1));
   fprintf(stderr,"setting transport %s\n", w->transport);
}

/* one special sub-process runs the 'device': subscriber/central republisher */
void device(void) {
  int n,rc=-1;
  void *dev_context=NULL;
  void *pull_socket=NULL,*pub_socket=NULL;
  if ( !(dev_context = zmq_init(1))) goto done;
  if ( !(pull_socket = zmq_socket(dev_context, ZMQ_PULL))) goto done;

  if (push_mode) {
    if ( !(pub_socket = zmq_socket(dev_context, ZMQ_PUSH))) goto done;
  } else {
    if ( !(pub_socket = zmq_socket(dev_context, ZMQ_PUB))) goto done;
  }

  /* connect the subscriber socket to each of the workers. then subscribe it */
  for(n=1;n<wn;n++) {
    int attempts=0;
    do { 
      rc = zmq_connect(pull_socket,workers[n].transport);
      if (rc) sleep(1);
    } while(rc && (attempts++ < 10));
    if (rc) {
        fprintf(stderr,"could not zmq_connect to %s after %d attempts\n",
            workers[n].transport, attempts);
        goto done;
    }
  }
  /* don't backlog infinite outbound messages when no subs present */
  if (zmq_setsockopt(pub_socket, ZMQ_SNDHWM, &hwm, sizeof(hwm))) goto done;

  /* one central publisher socket for external kvsp-sub's to get spools from */
  if (zmq_bind(pub_socket, pub_transport) == -1) goto done;

  /* central loop; this thing never exits unless exceptionally */
  while(1) {
    zmq_msg_t msg;
    zmq_msg_init(&msg);
    if ((rc = zmq_recvmsg(pull_socket,&msg,0)) != 0) break;
    if ((rc = zmq_sendmsg(pub_socket,&msg,0)) != 0) break;
    zmq_msg_close(&msg);
  }

 done:
  if (rc) fprintf(stderr,"zmq: device %s\n", zmq_strerror(errno));
  if (pub_socket) zmq_close(pub_socket);
  if (pull_socket) zmq_close(pull_socket);
  if (dev_context) zmq_term(dev_context);
  exit(rc);  /* never return, we are a worker subprocess */
}

/* run in one sub-process for every spool that's being published */
void worker(int w) {
  int rc=-1;
  pid_t pid;

  assert(workers[w].pid == 0);

  if ( (pid = fork()) == -1) {
    printf("fork error\n"); 
    exit(-1); 
  }
  if (pid > 0) { /* parent. */
    /* record worker */
    workers[w].pid = pid;
    workers[w].start = time(NULL);
    return;
  } 

  /* child here */

  /* unblock all signals */
  sigset_t all;
  sigemptyset(&all);
  sigprocmask(SIG_SETMASK,&all,NULL);

  char name[16];
  snprintf(name,sizeof(name),"kvsp-pub: %s", w ? workers[w].dir : "device");
  prctl(PR_SET_NAME,name);
  prctl(PR_SET_PDEATHSIG, SIGHUP); // TODO clean shutdown on HUP

  if (w == 0) device(); // never returns

  void *_set = kv_set_new();
  void *sp = kv_spoolreader_new(workers[w].dir);
  if (!sp) {
    fprintf(stderr,"failed to open spool %s\n", workers[w].dir);
    goto done;
  }

  /* prepare for ZMQ publishing */
  void *pub_context;
  void *pub_socket;
  if ( (!(pub_context = zmq_init(1)))                       || 
     ( (!(pub_socket = zmq_socket(pub_context, ZMQ_PUSH)))) ||
     (zmq_bind(pub_socket, workers[w].transport) == -1)) {
    goto done;
  }

  while (kv_spool_read(sp,_set,1) > 0) { /* read til interrupted by signal */
    kvset_t *set = (kvset_t*)_set; 
    assert(set->img && set->len);

    zmq_msg_t part;

    rc = zmq_msg_init_size(&part, set->len); assert(!rc);
    memcpy(zmq_msg_data(&part), set->img, set->len);
    rc = zmq_sendmsg(pub_socket, &part, 0);
    zmq_msg_close(&part);
    if(rc) goto done;
  }
  fprintf(stderr,"kv_spool_read exited (signal?)\n");

  rc=0;

 done:
  // TODO avoid printing to parent stderr  (kv lib does; so dup fd to logging)
  if (rc) fprintf(stderr,"zmq: %s %s\n", pub_transport, zmq_strerror(errno));
  if (pub_socket) zmq_close(pub_socket);
  if (pub_context) zmq_term(pub_context);
  if (sp) kv_spoolreader_free(sp);
  kv_set_free(_set);
  exit(rc);  /* do not return */
}

void run_workers() {
  int n;
  for(n=0; n < wn; n++) {
    if (workers[n].pid) continue;
    worker(n);
  }
}


void fini_workers() {
  int n,es;
  for(n=0; n < wn; n++) {
    worker_t *w = &workers[n];
    if (w->pid == 0) goto release;
    kill(w->pid, SIGTERM);
    if (waitpid(w->pid, &es, WNOHANG) == w->pid) w->pid = 0;
    else { /* child didn't exit. give it a moment, then force quit. */
      sleep(1);
      kill(w->pid, SIGKILL);
      if (waitpid(w->pid, &es, WNOHANG) == w->pid) w->pid = 0;
    }
    if(w->pid) fprintf(stderr, "can't terminate pid %d (%s): %s\n",(int)w->pid,w->dir,strerror(errno));
    else if (WIFSIGNALED(es)) fprintf(stderr,"exited on signal %d", (int)WTERMSIG(es));
    else if (WIFEXITED(es)) fprintf(stderr,"exit status %d", (int)WEXITSTATUS(es));

   release:
    free(w->dir);
    free(w->transport);
  }
}
 
void read_conf(char *file) {
  char line[200], *linep = line;
  int len;
  FILE *f;

  if ( (f = fopen(file,"r")) == NULL) {
      fprintf(stderr,"can't open %s: %s\n", file, strerror(errno));
      exit(-1);
  }
  while (fgets(line,sizeof(line),f) != NULL) {
    len = strlen(line);
    if (len && (line[len-1]=='\n')) line[--len] = '\0';
    if (len) utarray_push_back(dirs,&linep);
  }
}

int main(int argc, char *argv[]) {
  char *file, *dir;
  int n,opt,es,defer_restart;
  pid_t pid;

  utarray_new(dirs,&ut_str_icd);

  while ( (opt = getopt(argc, argv, "v+sf:d:")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 's': push_mode++; break;
      case 'f': file=optarg; read_conf(file); break;
      case 'd': dir=optarg; utarray_push_back(dirs,&dir); break;
      default: usage(argv[0]); break;
    }
  }
  if (optind < argc) pub_transport = argv[optind++];
  if (!pub_transport) usage(argv[0]);
  if ( (wn += utarray_len(dirs)) == 1) {
    fprintf(stderr,"no directories configured\n");
    usage(argv[0]);
  }

  if ( (workers = calloc(wn,sizeof(worker_t))) == NULL) exit(-1);
  for(n=1; n<wn; n++) configure_worker(n);

  /* block all signals. we stay blocked always except in sigwaitinfo */
  sigset_t all;
  sigfillset(&all);
  sigprocmask(SIG_SETMASK,&all,NULL);

  run_workers();

  while(1) {
    int signo = sigwaitinfo(&all,NULL);
    switch(signo) {
        case SIGCHLD:
          /* loop over children that have exited */
          defer_restart=0;
          while( (pid = waitpid(-1,&es,WNOHANG)) > 0) {
              for(n=0; n < wn; n++) if (workers[n].pid==pid) break;
              assert(n != wn);
              int elapsed = time(NULL) - workers[n].start;
              if (elapsed < SHORT_DELAY) defer_restart=1;
              printf("pid %d exited after %d seconds: ", (int)pid, elapsed);
              if (WIFEXITED(es)) printf("exit status %d\n", (int)WEXITSTATUS(es));
              else if (WIFSIGNALED(es)) printf("signal %d\n", (int)WTERMSIG(es));
              workers[n].pid = 0;
          }
          if (defer_restart) {
            fprintf(stderr,"workers restarting too fast, delaying\n"); 
            alarm(SHORT_DELAY); 
          }
          else run_workers();
          break;
        case SIGALRM:
          run_workers();
          break;
        default:
          printf("got signal %d\n", signo);
          goto done;
          break;
    }
  }

 done:
  fini_workers();
  free(workers);
  utarray_free(dirs);
  return 0;
}
