#include <stdio.h>
#include <time.h>
#include "ts.h"

void show(int *i) { printf("%u\n", *i); }

const ts_mm mm = {.sz=sizeof(int),
                  .data=NULL,
                  .show=(ts_show_f*)show };
int main() {
  time_t i=0;
  int two=2;

  ts_t *t = ts_new(10,10,&mm); ts_show(t);
  for(i=0; i < 100; i += 10) ts_add(t, i, NULL); ts_show(t);
  for(i=0; i < 100; i += 10) ts_add(t, i, &two); ts_show(t);
  ts_free(t);
  return 0;
}
