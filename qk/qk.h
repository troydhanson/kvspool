#ifndef _QK_H_
#define _QK_H_

#include "utvector.h"
#include "utstring.h"

struct qk {
  UT_vector /* of UT_string */ keys;
  UT_vector /* of UT_string */ vals;

  /* the callback below is invoked on qk_end. it receives this struct.
   * the tmp is reserved for the callback to use a scratch space. the
   * data argument is opaque and is for passing state to the callback. */
  int (*cb)(struct qk *);
  UT_string tmp;
  void *data;

};

/* API */
struct qk *qk_new(void);
int qk_start(struct qk *qk);
int qk_add(struct qk *qk, char *key, char *val, ...);
int qk_end(struct qk *qk);
void qk_free(struct qk *qk);

#endif // _QK_H_
