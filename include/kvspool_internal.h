#ifndef _KVSPOOL_INTERNAL_H_
#define _KVSPOOL_INTERNAL_H_

#include "kvspool.h"
#include "utarray.h"

/*******************************************************************************
 * internal structures and prototypes 
 ******************************************************************************/
/* magic prefix at the start of the spool file */
#define KVSPOOL_MAGIC "KV+SPOOL"

/* (reader): scan for new readable spools when > x seconds elapse */
#define KVSPOOL_RESCAN_INTERVAL 1    /* seconds */

/* (writer) attrition as needed to keep the total spool size approx under x */
/* 1 GB is a default but this can be configured using the 'limits' file. */
#define KVSPOOL_DIR_MAX  (1*1024*1024*1024)  

/* individual files within the spool directory are limited also, by default
 * to size (KVSPOOL_DIR_MAX / 10). However we need to cap this at <4GB since
 * we use a 32-bit offset (stored in the .sr file) to mark the read position */
#define KVSPOOL_FILE_MAX (1*1024*1024*1024)

void sp_oom(void);
int sp_readdir(const char *dir, const char *suffix, UT_array *files);
int sp_strsort(const void *_a, const void *_b);
int kv_write_raw_frame(void *sp, void *img, size_t len);
void sp_readlimits(const char *dir);
void sp_attrition(char *dir);
void sp_keep_maxseq(UT_array *files);
typedef struct { 
  void *img;  /* the img and len refer to the serialized image of the kvset */
  size_t len; /* populated when a set is read from spool or publisher */
  kv_t *kvs; 
} kvset_t;
#endif
