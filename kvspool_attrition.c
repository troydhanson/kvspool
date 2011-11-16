#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <stdio.h>

#include "utarray.h"

#include "kvspool.h"
#include "kvspool_internal.h"

typedef struct { 
  int file_idx;
  int wr_locked;
  int consumed;
  struct stat sb;
} kv_spool_ainfo_t;

static UT_icd ainfo_icd = {sizeof(kv_spool_ainfo_t), NULL, NULL, NULL };
static int attrition_sort(const void *_a, const void *_b) {
  kv_spool_ainfo_t *a = (kv_spool_ainfo_t*)_a;
  kv_spool_ainfo_t *b = (kv_spool_ainfo_t*)_b;
  /* writer-locked ones sort after unlocked ones */
  if (a->wr_locked != b->wr_locked) return (a->wr_locked - b->wr_locked);
  /* consumed ones sort _before_ unconsumed ones */
  if (a->consumed != b->consumed) return (b->consumed - a->consumed);
  /* oldest modify time sorts first */
  return a->sb.st_mtime - b->sb.st_mtime;
}

static uint32_t get_rpos(char *sp_file) {
  uint32_t rpos=0;
  char *sr_file;
  int fd=-1, len;

  sr_file = strdup(sp_file);
  len = strlen(sr_file);
  sr_file[len-1] = 'r';
  if ( (fd = open(sr_file,O_RDONLY)) == -1) goto done;
  if (read(fd,&rpos,sizeof(rpos)) != sizeof(rpos)) goto done;
  /* success */

 done:
  free(sr_file);
  if (fd != -1) close(fd);
  return rpos;
}

static void sp_get_attrition_order(UT_array *files, UT_array *ainfo, 
                                         size_t *spool_sz) {
  kv_spool_ainfo_t *ai;
  char **f, *file;
  int fd;

  *spool_sz = 0;

  f=NULL;
  while ( (f=(char**)utarray_next(files,f))) {
    file = *f;
    utarray_extend_back(ainfo);
    ai = (kv_spool_ainfo_t*)utarray_back(ainfo);
    ai->file_idx = utarray_eltidx(files,f);
    if (stat(file, &ai->sb) == -1) continue;
    *spool_sz += ai->sb.st_size;
    ai->consumed = (get_rpos(file) >= ai->sb.st_size) ? 1 : 0;
    /* test whether a writer has a lock on this spool file */
    ai->wr_locked = 0;
    if ( (fd = open(file,O_RDONLY)) != -1) {
      if (flock(fd,LOCK_EX|LOCK_NB) == -1) ai->wr_locked=1;
      close(fd);
    }
  }
  utarray_sort(ainfo, attrition_sort);
}

/* periodic spool attrition. caps the collective size of the spool directory.
 * if reached, unlink spool files. prefer to unlink consumed spools, otherwise
 * unconsumed spools. Those having oldest last-modified timestamps go first.
 */
void sp_attrition(char *dir) {
  sp_readlimits(dir); /* reread in case the limits file was updated by hand */
  if (kv_spool_options.dir_max == 0) return; /* unlimited - never attrition */
  kv_spool_ainfo_t *ai;
  char **f, *file;
  size_t spool_sz, implied_max;
  UT_array *files=NULL, *ainfo=NULL;
  utarray_new(files, &ut_str_icd);
  sp_readdir(dir, ".sp", files);
#if 0
  /* bail out without having to 'stat' all the files in the spool, if we can */
  implied_max = utarray_len(files) * kv_spool_options.file_max;
  if (implied_max < kv_spool_options.dir_max) goto done;
  /* enough files to justify tallying up their size and attrition as needed */
#endif
  utarray_new(ainfo, &ainfo_icd);
  sp_get_attrition_order(files, ainfo, &spool_sz);
  if (spool_sz < kv_spool_options.dir_max) goto done;
  /* cull as many files as needed to bring spool size down below the cap */
  ai=NULL;
  while ( (ai=(kv_spool_ainfo_t*)utarray_next(ainfo,ai))) {
    f = (char**)utarray_eltptr(files, ai->file_idx); assert(f);
    file = *f;
    //fprintf(stderr,"unlinking %s of size %lu\n", file, (long)ai->sb.st_size);
    if (unlink(file) == 0) {
      spool_sz -= ai->sb.st_size;
      /* also unlink the .sr file */
      file[strlen(file)-1]='r'; 
      unlink(file); 
    }
    if (spool_sz < kv_spool_options.dir_max) break;
  }

 done:
  if (files) utarray_free(files);
  if (ainfo) utarray_free(ainfo);
}

