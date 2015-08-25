#include <stdarg.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include "qk.h"

/*******************************************************************************
 * plumbing for utvector of string
 ******************************************************************************/
void _utstring_init(void *_buf, unsigned num) {
  UT_string *s = (UT_string*)_buf;
  while(num--) utstring_init(&s[num]);
}
void _utstring_fini(void *_buf, unsigned num) {
  UT_string *s = (UT_string*)_buf;
  while(num--) utstring_done(&s[num]);
}
void _utstring_copy(void *_dst, void *_src, unsigned num) {
  UT_string *dst = (UT_string*)_dst;
  UT_string *src = (UT_string*)_src;
  while(num--) utstring_concat( &dst[num], &src[num] );
}
void _utstring_clear(void *_buf, unsigned num) {
  UT_string *s = (UT_string*)_buf;
  while(num--) utstring_clear(&s[num]);
}
static UT_vector_mm utvector_utstring_mm = {
  .sz = sizeof(UT_string),
  .init =  _utstring_init,
  .fini =  _utstring_fini,
  .copy =  _utstring_copy,
  .clear = _utstring_clear,
};
/*******************************************************************************
 * end of plumbing 
 ******************************************************************************/


struct qk *qk_new(void) {
  struct qk *qk = malloc(sizeof(*qk));
  if (qk == NULL) goto done;
  memset(qk,0,sizeof(*qk));
  utvector_init(&qk->keys, &utvector_utstring_mm);
  utstring_init(&qk->tmp);
 done:
  return qk;
}

int qk_start(struct qk *qk) {
  utvector_clear(&qk->keys);
  utstring_clear(&qk->tmp);
}

int qk_end(struct qk *qk) {
  if (qk->cb == NULL) return;
  return qk->cb(qk);
}

int qk_add(struct qk *qk, char *key, ...) {
  va_list ap;
  va_start(ap,key);
  UT_string *k = (UT_string*)utvector_extend(&qk->keys);
  utstring_printf_va(k,key,ap);
  va_end(ap);
}

void qk_free(struct qk *qk) {
  utvector_fini(&qk->keys);
  utstring_done(&qk->tmp);
  free(qk);
}

