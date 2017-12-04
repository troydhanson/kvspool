#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include "kvspool.h"

int iterations=1;
int iter_delay=10;
int verbose=0;
char *dir = NULL;

void usage(char *exe) {
  fprintf(stderr,"usage: %s [-v] [-f] [-i iterations] [-d delay] <dir>\n", exe);
  exit(-1);
}

int main(int argc, char *argv[]) {

  int opt;
  char *exe = argv[0];
  void *set;

  while ( (opt = getopt(argc, argv, "i:d:v+")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'i': iterations=atoi(optarg); break;
      case 'd': iter_delay=atoi(optarg); break;
      default: usage(exe); break;
    }
  }
  if (optind < argc) dir=argv[optind++];
  else usage(exe);

  void *sp = kv_spoolwriter_new(dir);
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
    /* kv_adds(set, "when", timebuf); */
    kv_adds(set, "iter", iterbuf);

    /* put one of every kind of data */
    kv_adds(set, "test_i8", iterbuf);
    kv_adds(set, "test_i16", iterbuf);
    kv_adds(set, "test_i32", iterbuf);
    kv_adds(set, "test_ipv4", "192.168.1.1");
    kv_adds(set, "test1_ipv46", "fe80::20c:29ff:fe99:c21b");
    kv_adds(set, "test2_ipv46", "ff02::1");
    kv_adds(set, "test3_ipv46", "224.0.0.1");
    kv_adds(set, "test_str", "hello");
    kv_adds(set, "test_str8", "world!");
    kv_adds(set, "test_d64", "3.14159");
    kv_adds(set, "test_mac", "00:11:22:33:44:55:66");

    kv_spool_write(sp,set);
    kv_set_free(set);
    if (iterations) sleep(iter_delay);
  }

  kv_spoolwriter_free(sp);
  return 0;
}

