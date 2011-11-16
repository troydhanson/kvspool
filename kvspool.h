#ifndef _KVSPOOL_H_
#define _KVSPOOL_H_

#include <stdio.h>
#include "uthash.h"

/* kvspool: an API for dealing with a set of key-value pairs */

/******************************************************************************
 * key-value set API 
 *****************************************************************************/
typedef struct {
  char *key;
  int klen;
  char fmt; /* format of the value; s/i/... */
  char *val;
  int vlen;
  UT_hash_handle hh; /* internal */
} kv_t;


#if defined __cplusplus
extern "C" {
#endif

void* kv_set_new(void);
void kv_set_free(void*);
void kv_set_clear(void*);
void kv_set_dump(void *set,FILE *out);
void kv_add(void*set, const char *key, int klen, const char *val, int vlen);
void kv_addt(void*set, const char *key, char fmt, const void *val, int vlen);
kv_t *kv_get(void*set, char *key);
#define kv_adds(set, key, val) kv_add(set,key,strlen(key),val,strlen(val))
int kv_len(void*set);
kv_t *kv_next(void*set,kv_t *kv);

/******************************************************************************
 * spooling API 
 *****************************************************************************/
void *kv_spoolreader_new(const char *dir);
int kv_spool_read(void*sp, void *set, int blocking);
void kv_spoolreader_free(void*);

void *kv_spoolwriter_new(const char *dir);
int kv_spool_write(void*sp, void *set);
void kv_spoolwriter_free(void*);
void sp_attrition(char *dir);

/******************************************************************************
 * special purpose API 
 *****************************************************************************/
typedef struct { int pct_consumed; } kv_stat_t;
int kv_stat(const char *dir, kv_stat_t *stats);
void sp_reset(const char *dir);

typedef struct {
  size_t dir_max;
  size_t file_max; /* this will be automatically set to 10% of dir_max */
} kv_spool_options_t;
extern kv_spool_options_t kv_spool_options;

#if defined __cplusplus
}
#endif

#endif
