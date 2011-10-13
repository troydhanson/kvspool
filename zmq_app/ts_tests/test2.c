#include <stdio.h>
#include <time.h>
#include "ts.h"

void insert(char *cur, char *incr) { *cur += *incr; }
void show(char *i) { printf("%u\n", (unsigned)*i); }

const ts_mm mm = {.sz=sizeof(char),
                  .data=(ts_data_f*)insert,
                  .show=(ts_show_f*)show };
int main() {
  time_t i=0;
  char one=1;

  ts_t *t = ts_new(10,10,&mm); ts_show(t);
  for(i=0; i < 100; i += 10) ts_add(t, i, &one); ts_show(t);
  ts_add(t,100,&one); ts_show(t);
  ts_add(t,120,&one); ts_show(t);
  ts_add(t,555,&one); ts_show(t);
  ts_free(t);
  return 0;
}
