#include <stdio.h>
#include <inttypes.h>
#include <time.h>
#include "ts.h"

void insert(uint64_t *cur, uint64_t *incr) { *cur += *incr; }
void show(uint64_t *i) { printf("%lu\n", (long)*i); }

const ts_mm mm = {.sz=sizeof(uint64_t),
                  .data=(ts_data_f*)insert,
                  .show=(ts_show_f*)show };
int main() {
  time_t i=0;
  uint64_t one=1;

  ts_t *t = ts_new(10,10,&mm); ts_show(t);
  for(i=0; i < 100; i += 10) ts_add(t, i, &one); ts_show(t);
  ts_add(t,100,&one); ts_show(t);
  ts_add(t,120,&one); ts_show(t);
  ts_add(t,555,&one); ts_show(t);
  ts_free(t);
  return 0;
}
