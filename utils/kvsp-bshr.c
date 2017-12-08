#include <stdio.h>
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
#include <arpa/inet.h>
#include "utarray.h"
#include "utstring.h"
#include "kvspool.h"
#include "kvsp-bconfig.h"
#include "shr.h"

/* read spool, cast to binary, write to shr */

int verbose;
int oneshot=0;
char *spool;
char *shr_file;

void usage(char *prog) {
  fprintf(stderr, "usage: %s -[vo] -b <config> -d <spool> -s <shr>\n", prog);
  fprintf(stderr, "          -o (oneshot) process one frame and exit\n");
  exit(-1);
}

int main(int argc, char *argv[]) {
  struct shr *shr = NULL;
  void *sp=NULL;
  void *set=NULL;
  int opt,rc=-1,sc;
  char *config_file, *b;
  size_t l;
  set = kv_set_new();
  utarray_new(output_keys, &ut_str_icd);
  utarray_new(output_defaults, &ut_str_icd);
  utarray_new(output_types,&ut_int_icd);
  UT_string *tmp;
  utstring_new(tmp);

  while ( (opt = getopt(argc, argv, "v+d:b:os:")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'o': oneshot=1; break;
      case 'd': spool=strdup(optarg); break;
      case 's': shr_file=strdup(optarg); break;
      case 'b': config_file=strdup(optarg); break;
      default: usage(argv[0]); break;
    }
  }
  if (spool == NULL) usage(argv[0]);
  if (shr_file == NULL) usage(argv[0]);
  if (parse_config(config_file) < 0) goto done;
  shr = shr_open(shr_file, SHR_WRONLY);
  if (shr == NULL) goto done;

  sp = kv_spoolreader_new(spool);
  if (!sp) goto done;

  while (kv_spool_read(sp,set,1) > 0) {
    if (set_to_binary(set,tmp) < 0) goto done;

    b = utstring_body(tmp);
    l = utstring_len(tmp);
    sc = shr_write(shr, b, l);
    if (sc <= 0) {
      fprintf(stderr,"shr_write: error %d\n", sc);
      goto done;
    }
    if (oneshot) break;
  }

  rc = 0;

 done:
  if (sp) kv_spoolreader_free(sp);
  if (shr) shr_close(shr);
  kv_set_free(set);
  utarray_free(output_keys);
  utarray_free(output_defaults);
  utarray_free(output_types);
  utstring_free(tmp);

  return 0;
}

