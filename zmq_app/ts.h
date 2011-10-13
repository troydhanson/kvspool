
typedef void (ts_data_f)(char *cur, char *add);
typedef void (ts_ctor_f)(void *elt, size_t sz);
typedef void (ts_dtor_f)(void *elt);
typedef void (ts_show_f)(void *elt);
typedef struct {
    size_t sz;
    ts_data_f *data;
    ts_ctor_f *ctor;
    ts_dtor_f *dtor;
    ts_show_f *show;
} ts_mm;

typedef struct {
  time_t start;
  char data[]; /* C99 flexible array member */
} ts_bucket;

#define bkt(t,i) ((ts_bucket*)((char*)((t)->buckets) + ((i)*(sizeof(ts_bucket)+(t)->mm.sz))))
typedef struct {
  ts_mm mm;
  unsigned secs_per_bucket;
  unsigned num_buckets;
  ts_bucket *buckets;
} ts_t;

ts_t *ts_new(unsigned num_buckets, unsigned secs_per_bucket, const ts_mm *mm);
void ts_add(ts_t *t, time_t when, void *data);
void ts_free(ts_t *t);
void ts_show(ts_t *t);

