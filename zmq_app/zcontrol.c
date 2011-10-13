#include <assert.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <stdio.h>
#include "zcontrol.h"
#include "utstring.h"
#include "uthash.h"

typedef struct {  // wraps a command structure 
  cp_cmd_t cmd;
  UT_hash_handle hh;
  void *data;
} cp_cmd_w;

typedef struct {
  cp_cmd_w *cmds; // hash table of commands
  void *data;     // opaque data pointer passed into commands
  // fields used during command execution. only one command is
  // executing at a time because cp_exec is intended for use 
  // from only one thread (particularly the 'main' thread)
  cp_arg_t arg;
  zmq_msg_t *msgs;
  int nmsgs;
} cp_t;

static int help_cmd(void *_cp, cp_arg_t *arg, void *data) {
  cp_t *cp = (cp_t*)_cp;
  UT_string *t;
  utstring_new(t);
  cp_cmd_w *cw, *tmp;
  HASH_ITER(hh, cp->cmds, cw, tmp) {
    utstring_printf(t, "%-20s ", cw->cmd.name);
    utstring_printf(t, "%s\n",   cw->cmd.help);
  }
  utstring_printf(t, "%-20s %s\n", "quit", "close session");
  cp_add_reply(cp,utstring_body(t),utstring_len(t));
  utstring_free(t);
  return 0; 
}

static int unknown_cmd(void *cp, cp_arg_t *arg, void *data) {
  char unknown_msg[] = "command not found\n";
  if (arg->argc == 0) return 0;  /* no command; no-op */
  cp_add_reply(cp, unknown_msg, sizeof(unknown_msg)-1);
  return -1;
}
// unknown is not registered in the commands hash table
// so we point to this record if we need to invoke it.
static cp_cmd_w unknown_cmdw = {{"unknown",unknown_cmd}};

void *cp_init(cp_cmd_t *cmds, void *data) {
  cp_cmd_t *cmd;
  cp_t *cp;
  
  if ( (cp=calloc(1,sizeof(cp_t))) == NULL) goto done;
  cp->data = data;
  cp_add_cmd(cp, "help", help_cmd, "this text", NULL);
  for(cmd=cmds; cmd && cmd->name; cmd++) {
    cp_add_cmd(cp,cmd->name,cmd->cmdf,cmd->help,data);
  }

 done:
  return cp;
}

void cp_add_cmd(void *_cp, char *name, cp_cmd_f *cmdf, char *help, void *data) {
  cp_t *cp = (cp_t*)_cp;
  cp_cmd_w *cw;

  /* create new command if it isn't in the hash; else update in place */
  HASH_FIND(hh, cp->cmds, name, strlen(name), cw);
  if (cw == NULL) {
    if ( (cw = malloc(sizeof(*cw))) == NULL) exit(-1);
    memset(cw,0,sizeof(*cw));
    cw->cmd.name = strdup(name);
    cw->cmd.help = help ? strdup(help) : strdup("");
    HASH_ADD_KEYPTR(hh, cp->cmds, cw->cmd.name, strlen(cw->cmd.name), cw);
  }
  cw->cmd.cmdf = cmdf;
  cw->data = data;
}

#define alloc_msg(n,m) ((m)=realloc((m),(++(n))*sizeof(*(m))))
int cp_exec(void *_cp, void *rep) {
  cp_t *cp = (cp_t*)_cp;
  int rc=-1, i;
  cp_cmd_w *cw;
  void *tmp;
  zmq_msg_t *msgs=NULL; int nmsgs=0;
  int64_t more; size_t more_sz=sizeof(int64_t);

  assert(cp->arg.argc == 0);
  assert(cp->nmsgs==0);

  /* unpack the msg into argc/argv/lenv for the command callback */
  do {
    alloc_msg(nmsgs,msgs);
    zmq_msg_init(&msgs[nmsgs-1]);
    if (zmq_recv(rep,&msgs[nmsgs-1],0)) {
      fprintf(stderr,"zmq_recv: %s\n", zmq_strerror(errno));
      goto done;
    }
    if (zmq_getsockopt(rep, ZMQ_RCVMORE, &more, &more_sz)) more=0;
  } while(more);

  if (nmsgs == 0) {cw = &unknown_cmdw; goto run;}
  cp->arg.argc = nmsgs;
  cp->arg.argv = calloc(nmsgs,sizeof(char*));
  cp->arg.lenv = calloc(nmsgs,sizeof(size_t));
  if (!cp->arg.argv || !cp->arg.lenv) goto done;

  for(i=0; i < nmsgs; i++) {
    char *buf = zmq_msg_data(&msgs[i]);
    size_t sz = zmq_msg_size(&msgs[i]);
    if ( (cp->arg.argv[i] = malloc(sz+1)) == NULL) goto done;
    memcpy(cp->arg.argv[i], buf, sz);
    cp->arg.argv[i][sz]='\0';
    cp->arg.lenv[i] = sz;
  }

  /* find the command callback */
  HASH_FIND(hh, cp->cmds, cp->arg.argv[0], cp->arg.lenv[0], cw);
  if (!cw) cw = &unknown_cmdw;

  /* prepare the output area, run the callback */
 run:
  cw->cmd.cmdf(cp, &cp->arg, cw->data);

  /* send reply */
  if (cp->nmsgs == 0) { // special case: command generated no reply, send empty
    zmq_msg_t msg;
    zmq_msg_init_size(&msg,0);
    zmq_send(rep, &msg, 0);
    zmq_msg_close(&msg);
  }
  for(i=0; i < cp->nmsgs; i++) { // normal case, send non-empty command reply
    if (zmq_send(rep, &cp->msgs[i], (i<(cp->nmsgs-1))?ZMQ_SNDMORE:0)) {
      fprintf(stderr,"zmq_send: %s\n", zmq_strerror(errno));
      goto done;
    }
  }

  rc = 0; // success

 done:
  while (nmsgs) zmq_msg_close(&msgs[--nmsgs]);
  while (cp->nmsgs) zmq_msg_close(&cp->msgs[--cp->nmsgs]); cp->msgs=NULL;
  while(cp->arg.argc) free(cp->arg.argv[--cp->arg.argc]);
  if (cp->arg.argv) { free(cp->arg.argv); cp->arg.argv=NULL; }
  if (cp->arg.lenv) { free(cp->arg.lenv); cp->arg.lenv=NULL; }
  return rc;
}

void cp_free(void *_cp) {
  cp_t *cp = (cp_t*)_cp;
  cp_cmd_w *cw, *tmp;
  HASH_ITER(hh, cp->cmds, cw, tmp) {
    HASH_DEL(cp->cmds, cw);
    free(cw->cmd.name);
    free(cw->cmd.help);
    free(cw);
  }
  free(cp);
}

void cp_add_reply(void *_cp, void *buf, size_t len) { 
  cp_t *cp = (cp_t*)_cp;
  alloc_msg(cp->nmsgs,cp->msgs);
  zmq_msg_t *msg = &cp->msgs[cp->nmsgs-1];
  zmq_msg_init_size(msg, len);
  memcpy(zmq_msg_data(msg),buf,len);
}

