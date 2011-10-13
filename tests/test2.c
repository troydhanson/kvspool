#include <stdio.h>
#include "kvspool.h"

int main() {
  void *set = kv_set_new();
  /* add some kv pairs */
  kv_adds(set, "hello", "world");
  kv_adds(set, "second", "life");
  kv_adds(set, "hello", "new");

  /* see if it worked- count kv pairs */
  int len = kv_len(set);
  printf("kv set has %d items\n", (int)len);

  /* loop over them */
  kv_t *kv = NULL;
  while ( (kv = kv_next(set, kv))) {
    printf("key [%.*s], val [%.*s]\n", kv->klen, kv->key, kv->vlen, kv->val);
  }
  kv_set_free(set);
  return 0;
}
