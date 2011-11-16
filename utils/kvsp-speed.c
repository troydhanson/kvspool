#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include "kvspool.h"

int frames=100000;
int verbose=0;
char *dir = "/tmp";

void usage(char *exe) {
  fprintf(stderr,"usage: %s [-v] [-i iterations] [<dir>]\n", exe);
  exit(-1);
}

struct timeval t1, t2;

int main(int argc, char *argv[]) {

  long elapsed_usec_w, elapsed_usec_r;
  int opt,i;
  char *exe = argv[0];
  while ( (opt = getopt(argc, argv, "i:v+")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'i': frames=atoi(optarg); break;
      default: usage(exe); break;
    }
  }
  if (optind < argc) dir=argv[optind++];

  void *sp = kv_spoolwriter_new(dir);
  if (!sp) exit(-1);

  char timebuf[100], iterbuf[10];
  time_t t = time(NULL);
  snprintf(timebuf,sizeof(timebuf),"%s",ctime(&t));
  timebuf[strlen(timebuf)-1] = '\0'; /* trim \n */
  snprintf(iterbuf,sizeof(iterbuf),"%d",frames);

  void *set = kv_set_new();
  kv_adds(set, "from", exe);
  kv_adds(set, "time", timebuf);
  kv_adds(set, "iter", iterbuf);

  /* write test */
  gettimeofday(&t1,NULL);
  for(i=0; i<frames; i++) kv_spool_write(sp,set);
  gettimeofday(&t2,NULL);

  elapsed_usec_w = ((t2.tv_sec * 1000000) + t2.tv_usec) - 
                   ((t1.tv_sec * 1000000) + t1.tv_usec);
  kv_spoolwriter_free(sp);

  /* read test */
  sp = kv_spoolreader_new(dir);
  gettimeofday(&t1,NULL);
  for(i=0; i<frames; i++) kv_spool_read(sp,set,0);
  gettimeofday(&t2,NULL);

  elapsed_usec_r = ((t2.tv_sec * 1000000) + t2.tv_usec) - 
                   ((t1.tv_sec * 1000000) + t1.tv_usec);

  kv_spoolreader_free(sp);

  printf("write: %d kfps\n", (int)(frames*1000/elapsed_usec_w));
  printf("read:  %d kfps\n", (int)(frames*1000/elapsed_usec_r));

  kv_set_free(set);
  return 0;
}

