#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <stdio.h>

#include "utstring.h"
#include "utarray.h"
#include "tpl.h"
#include "shr.h"

#include "kvspool.h"
#include "kvspool_internal.h"


/*******************************************************************************
 * Spool writer API
 ******************************************************************************/
void *kv_spoolwriter_new(const char *dir) {
  char path[PATH_MAX];
  struct shr *shr;
  snprintf(path, PATH_MAX, "%s/%s", dir, "data");
  shr = shr_open(path, SHR_WRONLY);
  return shr;
}

int kv_spool_write(void*_sp, void *_set) {
  struct shr *shr = (struct shr *)_sp;
  kvset_t *set = (kvset_t*)_set;
  tpl_node *tn = NULL;
  char *buf=NULL; 
  char *key, *val;
  size_t len;
  ssize_t sc;
  int rc=-1;

  /* generate frame */
  tn = tpl_map("A(ss)", &key, &val);
  kv_t *kv = NULL;
  while ( (kv = kv_next(set, kv))) {
    key = kv->key;
    val = kv->val;
    tpl_pack(tn,1);
  }

  tpl_dump(tn, TPL_MEM, &buf, &len);
  sc = shr_write(shr, buf, len);
  if (sc <= 0) {
    fprintf(stderr, "shr_write: error\n");
    goto done;
  }

  rc=0;

 done:
  tpl_free(tn);
  if (buf) free(buf);
  return rc;
}

int kv_spool_writeN(void *_sp, void **_setv, int nset) {
  struct shr *shr = (struct shr *)_sp;
  kvset_t **setv = (kvset_t**)_setv;
  struct iovec *iov=NULL;
  tpl_node *tn = NULL;
  int i, rc = -1, sc;
  char *key, *val;

  iov = malloc(sizeof(struct iovec) * nset);
  if (iov == NULL) {
    fprintf(stderr, "out of memory\n");
    goto done;
  }


  for(i=0; i < nset; i++) {
    tn = tpl_map("A(ss)", &key, &val);
    kv_t *kv = NULL;
    while ( (kv = kv_next(setv[i], kv))) {
      key = kv->key;
      val = kv->val;
      tpl_pack(tn,1);
    }
    tpl_dump(tn, TPL_MEM, &iov[i].iov_base, &iov[i].iov_len);
    tpl_free(tn);
    tn = NULL;
  }

  sc = shr_writev(shr, iov, nset);
  if (sc <= 0) {
    fprintf(stderr, "shr_writev: error\n");
    goto done;
  }
  
  rc = 0;
 
 done:
  if (iov) {
    for(i=0; i < nset; i++) free(iov[i].iov_base);
    free(iov);
  }
  return rc;
}

void kv_spoolwriter_free(void*_sp) {
  struct shr *shr = (struct shr *)_sp;
  shr_close(shr);
}
