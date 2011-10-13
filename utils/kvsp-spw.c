#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include "kvspool.h"

int iterations=1;
int iter_delay=10;
int verbose=0;
char *base = "spw";
char *dir = NULL;

void usage(char *exe) {
  fprintf(stderr,"usage: %s [-v] [-f] [-b base] [-i iterations] [-d delay] <dir>\n", exe);
  exit(-1);
}

int main(int argc, char *argv[]) {

  int opt;
  char *exe = argv[0];
  void *set;

  while ( (opt = getopt(argc, argv, "i:d:b:v+")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'i': iterations=atoi(optarg); break;
      case 'd': iter_delay=atoi(optarg); break;
      case 'b': base=strdup(optarg); break;
      default: usage(exe); break;
    }
  }
  if (optind < argc) dir=argv[optind++];
  else usage(exe);

  void *sp = kv_spoolwriter_new(dir,base);
  if (!sp) exit(-1);

  char timebuf[100], iterbuf[10];

  while(iterations--) {
    time_t t = time(NULL);
    unsigned t32 = (unsigned)t;
    snprintf(timebuf,sizeof(timebuf),"%s",ctime(&t));
    timebuf[strlen(timebuf)-1] = '\0'; /* trim \n */
    snprintf(iterbuf,sizeof(iterbuf),"%d",iterations);

    set = kv_set_new();
    kv_adds(set, "from", exe);
    kv_adds(set, "when", timebuf);
    /* add a binary value having the unix epoch time */
    kv_add(set, "time", strlen("time"), (char*)&t32, sizeof(t32));
    kv_adds(set, "iter", iterbuf);
    kv_spool_write(sp,set);
    kv_set_free(set);
    if (iterations) sleep(iter_delay);
  }

  kv_spoolwriter_free(sp);
  return 0;
}

