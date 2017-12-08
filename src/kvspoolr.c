#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/inotify.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <dirent.h>
#include "kvspool.h"
#include "kvspool_internal.h"
#include "utstring.h"
#include "tpl.h"
#include "shr.h"

static void fill_set(void *img, size_t sz, kvset_t *set) {
  tpl_node *tn;
  char *key;
  char *val;

  kv_set_clear(set);

  tn = tpl_map("A(ss)", &key, &val);
  if (tpl_load(tn, TPL_MEM, img, sz) == -1) {
    fprintf(stderr, "tpl_load failed (sz %d)\n", (int)sz);
    return;
  }
  while( tpl_unpack(tn,1) > 0 ) {
    kv_adds(set, key, val);
    free(key);
    free(val);
  }
  tpl_free(tn);
}

/*******************************************************************************
 * Spool reader API
 ******************************************************************************/
void *kv_spoolreader_new(const char *dir) {
  char path[PATH_MAX];
  struct shr *shr;
  snprintf(path, PATH_MAX, "%s/%s", dir, "data");
  shr = shr_open(path, SHR_RDONLY);
  return shr;
}

void *kv_spoolreader_new_nb(const char *dir, int *fd) {
  char path[PATH_MAX];
  struct shr *shr=NULL;
  int rc = -1;
  snprintf(path, PATH_MAX, "%s/%s", dir, "data");
  shr = shr_open(path, SHR_RDONLY|SHR_NONBLOCK);
  if (shr == NULL) goto done;

  if (fd) *fd = shr_get_selectable_fd(shr);

  rc = 0;

 done:
  return shr;
}

/* returns 1 if frame ready, 0 = no data (nonblocking), or -1 on error 
 * whether or not its a blocking or non-blocking read depends on the
 * way it was opened (kv_spoolreader_new or with _nb suffix)
 */
int kv_spool_read(void *_sp, void *_set, int obsolete_blocking_flag) {
  struct shr *shr = (struct shr *)_sp;
  kvset_t *set = (kvset_t*)_set;
  char buf[4096];
  ssize_t sc;

  sc = shr_read(shr, buf, sizeof(buf));
  if (sc > 0) {
    fill_set(buf, sc, set);
    return 1;
  }
  return sc; /* negative (error) or 0 (no data) case */
}

void kv_spoolreader_free(void *_sp) {
  struct shr *shr = (struct shr *)_sp;
  shr_close(shr);
}

/* get the percentage consumed for dir 
   returns -1 on error */
int kv_stat(const char *dir, kv_stat_t *stats) {
  struct shr *shr=NULL;
  char path[PATH_MAX];
  struct shr_stat s;
  int rc = -1;

  struct stat st;
  snprintf(path, PATH_MAX, "%s/%s", dir, "data");
  if (stat(path, &st) < 0) {
    fprintf(stderr, "stat: %s\n", strerror(errno));
    goto done;
  }

  shr = shr_open(path, SHR_RDONLY);
  if (shr_stat(shr, &s, NULL) < 0) goto done;

  stats->pct_consumed = 100 - ((100.0 * s.bu) / s.bn);
  /* last_write is actually last_mtime (read OR write) */
  stats->last_write = st.st_mtim.tv_sec;
  stats->spool_sz = s.bn;

  rc = 0;

 done:
  if (shr) shr_close(shr);
  return rc;
}
