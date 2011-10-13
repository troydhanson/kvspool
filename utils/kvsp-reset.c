#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "kvspool.h"

char *dir;
char *base;
int verbose;

void usage(char *exe) {
  fprintf(stderr,"usage: %s [-b base] <dir>\n", exe);
  exit(-1);
}

int main(int argc, char *argv[]) {

  int opt;
  char *exe = argv[0];
  while ( (opt = getopt(argc, argv, "b:v+")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'b': base=strdup(optarg); break;
      default: usage(exe); break;
    }
  }
  if (optind < argc) dir=argv[optind++];
  else usage(exe);

  sp_reset(dir,base);
  return 0;
}

