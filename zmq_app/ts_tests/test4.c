#include <stdio.h>
#include <inttypes.h>
#include <time.h>
#include "ts.h"

typedef struct {
  char c[7];
} seven_t;

void insert(seven_t *cur, seven_t *incr) { cur->c[0]++; }
void show(seven_t *i) { printf("%u\n", (unsigned)(i->c[0])); }

const ts_mm mm = {.sz=sizeof(seven_t),
                  .data=(ts_data_f*)insert,
                  .show=(ts_show_f*)show };
int main() {
  time_t i=0;

  ts_t *t = ts_new(10,10,&mm); ts_show(t);
  for(i=0; i < 100; i += 10) ts_add(t, i, NULL); ts_show(t);
  ts_add(t,100,NULL); ts_show(t);
  ts_add(t,120,NULL); ts_show(t);
  ts_add(t,555,NULL); //ts_show(t);
  ts_add(t,555,NULL); //ts_show(t);
  ts_add(t,556,NULL); //ts_show(t);
  ts_add(t,564,NULL); //ts_show(t);
  ts_add(t,575,NULL); ts_show(t);
  ts_free(t);
  return 0;
}
