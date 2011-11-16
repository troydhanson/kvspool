/* kvpy.c - by Troy Hanson
 */

#include "Python.h"
#include "kvspool.h"

static int dictionary_to_kvs(PyObject *dict, void *set) {
    PyObject *pk, *pv;
    int rc = -1;
    //Py_ssize_t pos = 0;
    /* Python 2.4 lacked Py_ssize_t */
    ssize_t pos = 0;
    char *k, *v;

    while (PyDict_Next(dict, &pos, &pk, &pv)) {
      k = PyString_AsString(pk);
      v = PyString_AsString(pv);
      if (!k || !v) goto done;
      kv_adds(set, k, v);
    }
    rc = 0;
   done:
    return rc;
}

static int kvs_to_dictionary(PyObject **_dict, void *set) {
    PyObject *pk, *pv;
    int rc = -1;

    PyObject *dict = PyDict_New();

    kv_t *kv=NULL;
    while ( (kv= kv_next(set,kv))) {

      pk = Py_BuildValue("s#",kv->key,(int)kv->klen);
      switch(kv->fmt) {
        case 'c': pv = Py_BuildValue("b",*(char*)kv->val); break;
        case 'm': pv = Py_BuildValue("h",*( int16_t*)kv->val); break;
        case 'n': pv = Py_BuildValue("H",*(uint16_t*)kv->val); break;
        case 'd': pv = Py_BuildValue("i",*( int32_t*)kv->val); break;
        case 'u': pv = Py_BuildValue("I",*(uint32_t*)kv->val); break;
        case 'D': pv = Py_BuildValue("l",*( int64_t*)kv->val); break;
        case 'U': pv = Py_BuildValue("L",*(uint64_t*)kv->val); break;
        case 'b': pv = Py_BuildValue("z#",kv->val,(int)kv->vlen); break;
        case 's': pv = Py_BuildValue("s#",kv->val,(int)kv->vlen); break;
        case 'f': pv = Py_BuildValue("d",*(double*)kv->val); break;
        default: 
          PyErr_SetString(PyExc_RuntimeError, "unsupported type in spool");
          rc = -1;
          goto done;
      }
      rc = PyDict_SetItem(dict,pk,pv);
      Py_DECREF(pk); Py_DECREF(pv);
      if (rc == -1) break;
    }

   done:
    if (rc == -1) {
      Py_DECREF(dict);
      dict = NULL;
    }
    *_dict = dict;
    return rc;
}


static PyObject *kvpy_read(PyObject *self, PyObject *args)
{
    char *dir;
    int block, rc;
    void *sp, *set;
    PyObject *dict = NULL;

    if (!PyArg_ParseTuple(args, "ssi:kvpy_read", &dir, &block)) {
        return NULL;
    }

    /* normalize inputs */
    block = block ? 1 : 0;

    /* try to read a spool frame */
    if ( (sp = kv_spoolreader_new(dir)) == NULL) {
      PyErr_SetString(PyExc_RuntimeError, "cannot initialize spool reader");
      return NULL;
    }

    set = kv_set_new();
    if ( (rc=kv_spool_read(sp,set,block)) > 0) {
      kvs_to_dictionary(&dict, set);
    } else if (rc == 0) {
      PyErr_SetString(PyExc_RuntimeError, "no frames available");
    } else if (rc < 0) {
      PyErr_SetString(PyExc_RuntimeError, "internal error in spool reader");
    }

    kv_set_free(set);
    kv_spoolreader_free(sp);
    return dict;
}

static PyObject *kvpy_write(PyObject *self, PyObject *args)
{
    char *dir;
    void *sp, *set;
    PyObject *dict;

    if (!PyArg_ParseTuple(args, "ssO:kvpy_write", &dir, &dict)) {
        return NULL;
    }

    /* try to write a spool frame */
    if ( (sp = kv_spoolwriter_new(dir)) == NULL) {
      PyErr_SetString(PyExc_RuntimeError, "cannot initialize spool writer");
      return NULL;
    }

    set = kv_set_new();
    if (dictionary_to_kvs(dict, set) == -1) {
      PyErr_SetString(PyExc_RuntimeError, "non-string key or value");
      return NULL;
    }
    kv_spool_write(sp,set);
    kv_set_free(set);
    kv_spoolwriter_free(sp);
    return Py_BuildValue("i", 1);
}

static PyObject *kvpy_stat(PyObject *self, PyObject *args)
{
    char *dir;
    int rc=0, sc, i;
    kv_stat_t stats;
    PyObject *dict = NULL;
    PyObject *pk, *pv;

    if (!PyArg_ParseTuple(args, "ss:kvpy_read", &dir)) {
        return NULL;
    }

    sc = kv_stat(dir,&stats);
    if (sc == -1) {
      PyErr_SetString(PyExc_RuntimeError, "kv_stat failed");
      return NULL;
    }

    dict = PyDict_New();
    pk = PyString_FromString("pct");
    pv = PyInt_FromLong((long)(stats.pct_consumed));
    rc = PyDict_SetItem(dict,pk,pv);
    Py_DECREF(pk);
    Py_DECREF(pv);
    if (rc == -1) {
      Py_DECREF(dict);
      dict = NULL;
    }

    return dict;
}

PyDoc_STRVAR(kvpy_kypy_read__doc__,
"kvpy_read(dir, block) -> dictionary\n\
dir is directory\n\
block is 1/0 indicating whether to wait for a frame if none is ready.");

PyDoc_STRVAR(kvpy_kypy_write__doc__,
"kvpy_write(dir, dict) \n\
dir is directory\n\
dict is the dictionary to spool out");

PyDoc_STRVAR(kvpy_kypy_stat__doc__,
"kvpy_stat(dir) -> dictionary\n\
dir is directory\n");


static PyMethodDef kvpy_methods[] = {
    {"kvpy_read",       kvpy_read,  METH_VARARGS, kvpy_kypy_read__doc__},
    {"kvpy_write",      kvpy_write, METH_VARARGS, kvpy_kypy_write__doc__},
    {"kvpy_stat",       kvpy_stat,  METH_VARARGS, kvpy_kypy_stat__doc__},
    {NULL,              NULL}           /* sentinel */
};

PyMODINIT_FUNC
initkvpy(void)
{
    Py_InitModule("kvpy", kvpy_methods);
}
