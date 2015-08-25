#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include "qk.h"

int dump(struct qk *qk) {
  size_t l = utvector_len(&qk->keys);

  utstring_clear(&qk->tmp);
  utstring_printf(&qk->tmp, "qk_end: %lu keys\n", l);

  UT_string *k=NULL;
  while(l--) {
    k = (UT_string*)utvector_next(&qk->keys,k); assert(k);
    utstring_printf(&qk->tmp, "%s\n", utstring_body(k));
  }
  char *out = utstring_body(&qk->tmp);
  size_t len =utstring_len( &qk->tmp);
  write(STDOUT_FILENO, out, len);
  return 0;
}

int main() {
  struct qk *qk = qk_new();

  qk->cb = dump;

  qk_start(qk);
  qk_add(qk, "A:%d", 1);
  qk_add(qk, "B:%d%c", 2, 'a');
  qk_add(qk, "C:%d%c", 3, 'b');
  qk_add(qk, "D:%s", "IV");
  qk_add(qk, "E:%lu", (long)5);
  qk_end(qk);

  qk_start(qk);
  qk_add(qk, "A:%d", 1);
  qk_add(qk, "B:%d%c", 2, 'a');
  qk_add(qk, "C:%d%c", 3, 'b');
  qk_add(qk, "D:%s", "IV");
  qk_end(qk);

  qk_free(qk);

  return 0;
}
