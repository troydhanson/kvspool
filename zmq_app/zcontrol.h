#ifndef CONTROL_PORT_H
#define CONTROL_PORT_H

#include <stdlib.h>
#include <zmq.h>

typedef struct {
  int     argc; /* conventional argc/argv args */
  char  **argv;
  size_t *lenv; // len of each arg, useful if client sends null-containing args
} cp_arg_t;

typedef int (cp_cmd_f)(void *cp, cp_arg_t *arg, void *data);

typedef struct {
  char *name;
  cp_cmd_f *cmdf;
  char *help;
} cp_cmd_t;

void *cp_init(cp_cmd_t *cmds, void *data);
void cp_free(void *_cp);
void cp_add_cmd(void*, char *name, cp_cmd_f *cmdf, char *help, void *data);
int cp_exec(void *_cp, void *zmq_reply_socket);

/* these are used within command callbacks */
#define cp_add_replys(a,b) cp_add_reply((a),(b),strlen(b))
void cp_add_reply(void *, void *buf, size_t len);

#endif 
