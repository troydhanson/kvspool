#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/types.h>
#include <fcntl.h>
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <zmq.h>

struct _CF {
  int run;
  int verbose;
  char *zpath; 
  char *prompt;
  void *zmq_context;
  void *req_socket;
} CF = {
  .run = 1,
  .zpath = "tcp://127.0.0.1:3333",
  .prompt = "zcon> ",
};

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-v] [-s server]\n", prog);
  fprintf(stderr, "          -v verbose\n");  
  fprintf(stderr, "          -s zmq address of server\n");  
  exit(-1);
}

/* This little parsing function finds one word at a time from the
 * input line. It supports double quotes to group words together. */
const int ws[256] = {[' ']=1, ['\t']=1};
char *find_word(char *c, char **start, char **end) {
  int in_qot=0;
  while ((*c != '\0') && ws[*c]) c++; // skip leading whitespace
  if (*c == '"') { in_qot=1; c++; }
  *start=c;
  if (in_qot) {
    while ((*c != '\0') && (*c != '"')) c++;
    *end = c;
    if (*c == '"') { 
      in_qot=0; c++; 
      if ((*c != '\0') && !ws[*c]) {
        fprintf(stderr,"text follows quoted text without space\n"); return NULL;
      }
    }
    else {fprintf(stderr,"quote mismatch\n"); return NULL;}
  }
  else {
    while ((*c != '\0') && (*c != ' ')) {
      if (*c == '"') {fprintf(stderr,"start-quote within word\n"); return NULL; }
      c++;
    }
    *end = c;
  }
  return c;
}

#define alloc_msg(n,m) ((m)=realloc((m),(++(n))*sizeof(*(m))))
int do_rqst(char *line) {
  char *c=line, *start=NULL, *end=NULL, *buf;
  int nmsgs=0; zmq_msg_t *msgs = NULL;
  int rmsgs=0; zmq_msg_t *msgr = NULL;
  size_t sz, more_sz=sizeof(int64_t);
  int64_t more=0; 
  int i, rc = -1;

  /* parse the line into argv style words, pack and transmit the request */
  while(*c != '\0') {
    if ( (c = find_word(c,&start,&end)) == NULL) goto done; // TODO confirm: normal exit?
    //fprintf(stderr,"[%.*s]\n", (int)(end-start), start);
    assert(start && end);
    alloc_msg(nmsgs,msgs);
    buf = start; sz = end-start;
    zmq_msg_init_size(&msgs[nmsgs-1],sz);
    memcpy(zmq_msg_data(&msgs[nmsgs-1]),buf,sz);
    start = end = NULL;
  }

  // send request
  for(i=0; i<nmsgs; i++) {
    if (zmq_send(CF.req_socket, &msgs[i], (i<nmsgs-1)?ZMQ_SNDMORE:0)) {
      fprintf(stderr,"zmq_send: %s\n", zmq_strerror(errno));
      goto done;
    }
  }

  // get reply 
  do {
    alloc_msg(rmsgs,msgr);
    zmq_msg_init(&rmsgs[msgr-1]);
    if (zmq_recv(CF.req_socket, &rmsgs[msgr-1], 0)) {
      fprintf(stderr,"zmq_recv: %s\n", zmq_strerror(errno));
      goto done;
    }
    buf = zmq_msg_data(&rmsgs[msgr-1]); 
    sz = zmq_msg_size(&rmsgs[msgr-1]); 
    printf("%.*s\n", (int)sz, (char*)buf);
    if (zmq_getsockopt(CF.req_socket, ZMQ_RCVMORE, &more, &more_sz)) more=0;
  } while (more);
  
  rc = 0;

 done:
  for(i=0; i<nmsgs; i++) zmq_msg_close(&msgs[i]);
  for(i=0; i<rmsgs; i++) zmq_msg_close(&msgr[i]);
  if (rc) CF.run=0;
  return rc;
}
 
int setup_zmq() {
  int rc=-1;
  CF.zmq_context = zmq_init(1);
  if (!CF.zmq_context) {
    fprintf(stderr,"zmq_init: %s\n",zmq_strerror(errno));
    goto done;
  }

  CF.req_socket = zmq_socket(CF.zmq_context, ZMQ_REQ);
  if (!CF.req_socket) {
    fprintf(stderr,"zmq_socket: %s\n",zmq_strerror(errno));
    goto done;
  }
  int zero=0;
  zmq_setsockopt(CF.req_socket, ZMQ_LINGER, &zero, sizeof(zero));

  fprintf(stderr,"Connecting to %s.\n", CF.zpath);
  if (zmq_connect(CF.req_socket, CF.zpath)) {
    fprintf(stderr,"zmq_connect: %s\n",zmq_strerror(errno));
    goto done;
  }

  rc = 0; // success

 done:
  return rc;
}

int main(int argc, char *argv[]) {
  struct sockaddr_un addr;
  int opt,rc,quit;
  char *line;

  while ( (opt = getopt(argc, argv, "v+s:")) != -1) {
    switch (opt) {
      case 'v': CF.verbose++; break;
      case 's': CF.zpath = strdup(optarg); break;
      default: usage(argv[0]); break;
    }
  }
  if (optind < argc) usage(argv[0]);
  if (setup_zmq()) goto done;
  using_history();

  while(CF.run) {
    line=readline(CF.prompt);
    quit = (!line) || (!strcmp(line,"exit")) || (!strcmp(line,"quit"));
    if (quit) CF.run=0;
    else if (*line != '\0') {
      add_history(line);
      do_rqst(line);
    }
    if (line) free(line); 
  }

 done:
  clear_history();
  if (CF.req_socket) zmq_close(CF.req_socket);
  if (CF.zmq_context) zmq_term(CF.zmq_context);
  return 0;
}

