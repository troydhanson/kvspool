quick key (qk)
--------------

This is a mini-kvspool that lets the application determine what to do with each
key-value dictionary that is produced.  First create one:

    struct qk *qk = qk_new();

Set up a callback to be invoked whenever you "end" a dictionary:

    qk->cb = your_callback; 

The callback has this prototype:

    int (*cb)(struct qk *);

The callback can use `qk->tmp` (a `UT_string`) as a scratch buffer.  It can
iterate over `qk->keys` and `qk->vals` (both of type `UT_vector` whose elements
are `UT_string`). Use this sequence to produce a dictionary:

    qk_start(qk);
    qk_add(qk, key, val, ...);
    ...
    qk_end(qk);

Call `qk_add` multiple times to add several key-value pairs to the dictionary.
Note `val` is a printf-style format string that can take additional arguments.
At program termination do this:

    qk_free(qk);
