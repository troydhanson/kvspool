#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include "kvspool_internal.h"

int block=1;
int verbose=0;
char *base = NULL;
char *dir = NULL;
enum {mode_out,mode_nop} mode = mode_out;

void usage(char *exe) {
  fprintf(stderr,"usage: %s [-c|-f] [-v] [-b base] [-B(lock) <1|0>] <dir>\n", exe);
  fprintf(stderr,"  MODES: default               (text mode)\n");
  fprintf(stderr,"         -f                    (discard mode)\n");
  exit(-1);
}

/******************************************************************************
 * kvsp-spr: a spool reader, for manually inspecting contents of a spool
 *****************************************************************************/
int main(int argc, char *argv[]) {

  char *exe = argv[0];
  int opt;

  while ( (opt = getopt(argc, argv, "cfB:b:v+p:")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'f': mode=mode_nop; break;
      case 'b': base=strdup(optarg); break;
      case 'B': block=atoi(optarg)?1:0; break;
      default: usage(exe); break;
    }
  }
  if (optind < argc) dir=argv[optind++];
  else usage(exe);

  void *sp = kv_spoolreader_new(dir,base);
  if (!sp) exit(-1);


  void *set = kv_set_new();
  while (kv_spool_read(sp,set,block) > 0) {
    switch(mode) {
      case mode_nop: continue;
      case mode_out: kv_set_dump(set,stdout); printf("\n"); break;
      default: assert(0); break;
    }
  }

 done:
  kv_spoolreader_free(sp);
  kv_set_free(set);
  return 0;
}
