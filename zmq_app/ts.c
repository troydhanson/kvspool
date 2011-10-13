#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include "ts.h"

static void ts_def_clear(char *data, size_t sz) { memset(data,0,sz); }
static void ts_def_incr(int *cur, int *incr) { *cur += (incr ? (*incr) : 1); }
static void ts_def_show(int *cur) { printf("%d\n",*cur); }

ts_t *ts_new(unsigned num_buckets, unsigned secs_per_bucket, const ts_mm *mm) {
  int i;
  ts_t *t = calloc(1,sizeof(ts_t)); if (!t) return NULL;
  t->secs_per_bucket = secs_per_bucket;
  t->num_buckets = num_buckets;
  t->mm = *mm; /* struct copy */
  // pad sz so sequential buckets' time_t are naturally aligned
  int pad = ((t->mm.sz % sizeof(ts_bucket)) > 0) ? 
            (sizeof(ts_bucket) - (t->mm.sz % sizeof(ts_bucket))) :
            0;
  t->mm.sz += pad;
  if (t->mm.ctor == NULL) t->mm.ctor = (ts_ctor_f*)ts_def_clear;
  if (t->mm.data == NULL) {
     if (mm->sz == sizeof(int)) t->mm.data = (ts_data_f*)ts_def_incr;
     else assert(0);
  }
  if (t->mm.show == NULL) {
     if (mm->sz == sizeof(int)) t->mm.show = (ts_show_f*)ts_def_show;
  }
  t->buckets = calloc(num_buckets,sizeof(ts_bucket)+t->mm.sz);
  if (t->buckets == NULL) { free(t); return NULL; }
  for(i=0; i<t->num_buckets; i++) {
    //fprintf(stderr,"t->buckets %p bkt(t,%d) %p\n", t->buckets, i, bkt(t,i));
    bkt(t,i)->start = i * t->secs_per_bucket;
    t->mm.ctor(bkt(t,i)->data,t->mm.sz);
  }
  return t;
}

void ts_add(ts_t *t, time_t when, void *data) {
  int i;
  if (bkt(t,0)->start > when) return; // too old
  /* figure out bucket it should go in */
  unsigned idx = (when - bkt(t,0)->start) / t->secs_per_bucket;
  if (idx >= t->num_buckets) { // shift
    unsigned shift = (idx - t->num_buckets) + 1;
    if (shift > t->num_buckets) shift = t->num_buckets;
    if (shift <= t->num_buckets) {
      if (t->mm.dtor) {
        for(i=0; i<shift; i++) t->mm.dtor(bkt(t,i)->data);
      }
    }
    if (shift < t->num_buckets) {
      memmove(bkt(t,0),bkt(t,shift),(t->num_buckets-shift)*(sizeof(ts_bucket)+t->mm.sz));
    }
    if (shift) {
      for(i=t->num_buckets-shift; i<t->num_buckets; i++) { 
        t->mm.ctor(bkt(t,i)->data,t->mm.sz);
        bkt(t,i)->start = (i > 0) ? (bkt(t,i-1)->start + t->secs_per_bucket) : when;
      }
    }
    idx = (when - bkt(t,0)->start) / t->secs_per_bucket;
    assert(idx < t->num_buckets);
  }
  void *cur = bkt(t,idx)->data;
  t->mm.data(cur,data);
}

void ts_free(ts_t *t) {
  int i;
  if (t->mm.dtor) {
    for(i=0; i<t->num_buckets; i++) t->mm.dtor(bkt(t,i)->data);
  }
  free(t->buckets);
  free(t);
}

void ts_show(ts_t *t) {
  int i;
  for(i=0; i<t->num_buckets; i++) {
    printf("#%d(%lu): ", i, (long)(bkt(t,i)->start));
    if (t->mm.show) t->mm.show(bkt(t,i)->data);
    else printf("\n");
  }
  printf("\n");
}
