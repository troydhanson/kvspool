#include <stdio.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <limits.h>
#include "shr.h"
#include "kvspool.h"

int frames=100000;
int verbose=0;
char *dir = "/dev/shm";
char path[PATH_MAX];
char *exe;

void usage(char *exe) {
  fprintf(stderr,"usage: %s [-v] [-i iterations] [<dir>]\n", exe);
  exit(-1);
}

int individual_frames_test() {
  long elapsed_usec_w, elapsed_usec_r;
  struct timeval t1, t2;
  int i;

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
  printf("writing and reading individual frames:\n");

  /* write test */
  gettimeofday(&t1,NULL);
  for(i=0; i<frames; i++) kv_spool_write(sp,set);
  gettimeofday(&t2,NULL);

  elapsed_usec_w = ((t2.tv_sec * 1000000) + t2.tv_usec) - 
                   ((t1.tv_sec * 1000000) + t1.tv_usec);
  kv_spoolwriter_free(sp);

  /* read test */
  sp = kv_spoolreader_new_nb(dir, NULL);
  gettimeofday(&t1,NULL);
  for(i=0; i<frames; i++) { 
    if (kv_spool_read(sp,set,1) < 1) {
      fprintf(stderr, "spool too small to hold %d frames for test\n", frames);
    }
  }
  gettimeofday(&t2,NULL);

  elapsed_usec_r = ((t2.tv_sec * 1000000) + t2.tv_usec) - 
                   ((t1.tv_sec * 1000000) + t1.tv_usec);

  kv_spoolreader_free(sp);

  printf("write: %d kfps\n", (int)(frames*1000/elapsed_usec_w));
  printf("read:  %d kfps\n", (int)(frames*1000/elapsed_usec_r));

  kv_set_free(set);
}

int batch_frames_test() {
  long elapsed_usec_w, elapsed_usec_r;
  struct timeval t1, t2;
  int sc, i, nset,total=0;

  void *sp = kv_spoolwriter_new(dir);
  if (!sp) exit(-1);

  char timebuf[100], iterbuf[10];
  time_t t = time(NULL);
  snprintf(timebuf,sizeof(timebuf),"%s",ctime(&t));
  timebuf[strlen(timebuf)-1] = '\0'; /* trim \n */

  void **setv = malloc(sizeof(void*) * frames);
  if (setv == NULL) {
    fprintf(stderr,"out of memory\n");
    return -1;
  }
  for(i=0; i<frames; i++) {
    setv[i] = kv_set_new();
    kv_adds(setv[i], "from", exe);
    kv_adds(setv[i], "time", timebuf);
    snprintf(iterbuf,sizeof(iterbuf),"%d",i);
    kv_adds(setv[i], "iter", iterbuf);
  }

  printf("writing and reading batch frames:\n");

  /* write test */
  nset = frames;
  gettimeofday(&t1,NULL);
  kv_spool_writeN(sp,setv, nset);
  gettimeofday(&t2,NULL);

  elapsed_usec_w = ((t2.tv_sec * 1000000) + t2.tv_usec) - 
                   ((t1.tv_sec * 1000000) + t1.tv_usec);
  kv_spoolwriter_free(sp);
  
  for(i=0; i<frames; i++) kv_set_clear(setv[i]);

  /* read test */
  sp = kv_spoolreader_new_nb(dir, NULL);
  gettimeofday(&t1,NULL);
  do {
    sc = kv_spool_readN(sp,setv,&nset);
    if (sc < 1) {
        fprintf(stderr, "spool empty\n");
        fprintf(stderr, "(may be too small to hold all frames written)\n");
        break;
    }
    if (nset != frames) fprintf(stderr, "read batch of %d frames\n", nset);
    total += nset;
  } while (total < frames);

  gettimeofday(&t2,NULL);

  elapsed_usec_r = ((t2.tv_sec * 1000000) + t2.tv_usec) - 
                   ((t1.tv_sec * 1000000) + t1.tv_usec);

  kv_spoolreader_free(sp);

  printf("write: %d kfps\n", (int)(frames*1000/elapsed_usec_w));
  printf("read:  %d kfps\n", (int)(frames*1000/elapsed_usec_r));

}

int main(int argc, char *argv[]) {
  int opt, sc, rc = -1;

  exe = argv[0];
  while ( (opt = getopt(argc, argv, "i:v+")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'i': frames=atoi(optarg); break;
      default: usage(exe); break;
    }
  }
  if (optind < argc) dir=argv[optind++];

  /* init the spool */
  snprintf(path, PATH_MAX, "%s/%s", dir, "data");
  sc = shr_init(path, 10*1024*1024, SHR_OVERWRITE|SHR_MESSAGES|SHR_DROP);
  if (sc < 0) goto done;

  sc = individual_frames_test();
  if (sc < 0) goto done;

  sc = batch_frames_test();
  if (sc < 0) goto done;

  rc = 0;

 done:
  return rc;
}

