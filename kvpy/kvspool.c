#include <Python.h>
#include <structmember.h>
#include "kvspool.h"

/* A Python object for using kvspool (as reader or writer)
 * 
 * USAGE:
 *   
 *  import kvspool
 *  sp = kvspool.Kvspool("/tmp/spool")
 *  dict = sp.read()
 *     or
 *  dict = {'day': 'today', 'weather':'sun'}
 *  sp.write(dict)
 *
 *  Implementation based on 
 *   http://docs.python.org/extending/newtypes.html#defining-new-types
 * 
 */

/* TODO
 *  - add non-blocking mode flag (S:i) in Kvspool_init arg parsing 
 *  - in non-blocking mode, don't throw an error on no-data, return None ?
 *  - in non-blocking mode, allow kv.blocking to be read-write (now readonly)
 *  - add tostr so that kvspool object can be printed (include r or w mode)
 *  - add support for python sequence protocol in reader mode, c.f.:
 *     http://docs.python.org/extending/newtypes.html#abstract-protocol-support
 * 
 *
*/


typedef struct {
  PyObject_HEAD
  PyObject *dir;
  void *set;
  void *spr;
  void *spw;
  int blocking;
} Kvspool;

static void 
Kvspool_dealloc(Kvspool *self)
{
  Py_XDECREF(self->dir);
  if (self->spr) kv_spoolreader_free(self->spr);
  if (self->spw) kv_spoolwriter_free(self->spw);
  if (self->set) kv_set_free(self->set);
  self->ob_type->tp_free((PyObject*)self);
}

/* the 'new' function is intended to create the space; later the 'init' function
 * fills in the new object. it's more complicated than this when pickling etc */
static PyObject *
Kvspool_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
  Kvspool *self;
  self = (Kvspool*)type->tp_alloc(type,0);
  if (!self) goto done;

  self->dir = PyString_FromString("");
  if (self->dir == NULL) { Py_DECREF(self); return NULL; }
  self->set = NULL;
  self->spr = NULL;
  self->spw = NULL;

 done:
  return (PyObject *)self;
}

static int 
Kvspool_init(Kvspool *self, PyObject *args, PyObject *kwds)
{
  PyObject *dir = NULL, *tmp;
  static char *kwlist[] = {"dir", NULL};

  if (! PyArg_ParseTupleAndKeywords(args, kwds, "S", kwlist, &dir)) return -1;

  if (dir) {
    tmp = self->dir;  // the justification for this way of setting dir is to
    Py_INCREF(dir);   // handle cases of subclasses with certain kinds of 
    self->dir = dir;  // deallocators - it's from the Python extending docs.
    Py_XDECREF(tmp);  // Whew.
  }
  self->set = kv_set_new();
  self->blocking = 1;
  return 0;
}

/* here's how we expose certain instance variables to python (e.g. kv.dir) */
static PyMemberDef Kvspool_members[] = {
  {"dir", T_OBJECT_EX, offsetof(Kvspool, dir), READONLY, "spool directory"},
  {"blocking", T_INT, offsetof(Kvspool, blocking), READONLY, "mode"},
  {NULL}
};

/* add a kv.name() method. right now the name is the spool directory. 
 * I'm not sure what circumstances name() is used or if its just an 
 * example from the Python extension docs; it seems to me that tostr
 * is a more useful thing to implement since that supports 'print kv' */
static PyObject *
Kvspool_name(Kvspool *self) 
{
  static PyObject *format;
  PyObject *args, *result;

  format = PyString_FromString("%s");
  if (format == NULL) return NULL;

  if (self->dir == NULL) {
    PyErr_SetString(PyExc_AttributeError, "dir");
    return NULL;
  }

  args = Py_BuildValue("O", self->dir);
  if (args == NULL) return NULL;

  result = PyString_Format(format, args);
  Py_DECREF(args);

  return result;
}

static int dictionary_to_kvs(PyObject *dict, void *set) {
    PyObject *pk, *pv;
    ssize_t pos = 0; /* Python 2.4 lacked Py_ssize_t */
    int rc = -1;
    char *k, *v;

    kv_set_clear(set);

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

PyDoc_STRVAR(Kvspool_read__doc__, "read() -> dictionary");
PyDoc_STRVAR(Kvspool_write__doc__, "write(dictionary)");

static PyObject *
Kvspool_read(Kvspool *self, PyObject *args) 
{
  PyObject *dict = NULL;
  int block=1;
  char *dir;
  int rc;


  /* first time reading? open spool reader, save the handle. */
  if (self->spr == NULL) {
    dir = PyString_AsString(self->dir);
    if ( (self->spr = kv_spoolreader_new(dir)) == NULL) {
      PyErr_SetString(PyExc_RuntimeError, "can't init spool reader");
      return NULL;
    }
  }

  /* try to read a spool frame. */
  if ( (rc = kv_spool_read(self->spr, self->set, block)) > 0) {
    kvs_to_dictionary(&dict, self->set);
  } else if (rc == 0) {
    /* only happens in non-blocking mode when no data is available */
    PyErr_SetString(PyExc_RuntimeError, "no data available");
  } else if (rc < 0) {
    PyErr_SetString(PyExc_RuntimeError, "internal error");
  }

  return dict;
}

static PyObject *
Kvspool_write(Kvspool *self, PyObject *args)
{
  PyObject *dict;
  char *dir;

  if (!PyArg_ParseTuple(args, "O:write", &dict)) return NULL;

  /* first time writing? open spool writer, save the handle. */
  if (self->spw == NULL) {
    dir = PyString_AsString(self->dir);
    if ( (self->spw = kv_spoolwriter_new(dir)) == NULL) {
      PyErr_SetString(PyExc_RuntimeError, "can't init spool writer");
      return NULL;
    }
  }

  if (dictionary_to_kvs(dict, self->set) == -1) {
    PyErr_SetString(PyExc_RuntimeError, "non-string key or value");
    return NULL;
  }

  if (kv_spool_write(self->spw, self->set) != 0) {
    PyErr_SetString(PyExc_RuntimeError, "write error");
    return NULL;
  }
  return Py_BuildValue(""); /* builds None (because NULL would throw error) */
}

static PyMethodDef Kvspool_methods[] = {
  {"read",    (PyCFunction)Kvspool_read,              METH_NOARGS,  Kvspool_read__doc__},
  {"write",   (PyCFunction)Kvspool_write,             METH_VARARGS, Kvspool_write__doc__},
  {"name",    (PyCFunction)Kvspool_name, METH_NOARGS,  "get spool directory"},
  {NULL,      NULL}
};

/* this kahuna defines a new python type; the type of our Kvspool object */
static PyTypeObject KvspoolType = {
  PyObject_HEAD_INIT(NULL)
  0,                             /* ob_size */
  "kvspool.Kvspool",             /* tp_name */
  sizeof(Kvspool),               /* tp_basicsize */
  0,                             /* tp_itemsize */
  (destructor)Kvspool_dealloc,   /* tp_dealloc */
  0,                             /* tp_print */
  0,                             /* tp_getattr */
  0,                             /* tp_setattr */
  0,                             /* tp_compare */
  0,                             /* tp_repr */
  0,                             /* tp_as_number */
  0,                             /* tp_as_sequence */
  0,                             /* tp_as_mapping */
  0,                             /* tp_hash */
  0,                             /* tp_call */
  0,                             /* tp_str */
  0,                             /* tp_getattro */
  0,                             /* tp_setattro */
  0,                             /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT,            /* tp_flags */
  "Kvspool object",              /* tp_doc  */
  0,                             /* tp_traverse */
  0,                             /* tp_clear */
  0,                             /* tp_richcompare */
  0,                             /* tp_weaklistoffset */
  0,                             /* tp_iter */
  0,                             /* tp_iternext */
  Kvspool_methods,               /* to_methods */
  Kvspool_members,               /* to_members */
  0,                             /* tp_getset */
  0,                             /* tp_base */
  0,                             /* tp_dict */
  0,                             /* tp_descr_get */
  0,                             /* tp_descr_set */
  0,                             /* tp_dictoffset */
  (initproc)Kvspool_init,        /* tp_init */
  0,                             /* tp_alloc */
  Kvspool_new,                   /* tp_new */
};

#ifndef PyMODINIT_FUNC /* declarations for DLL import/export */
#define PyMODINIT_FUNC void 
#endif
PyMODINIT_FUNC
initkvspool(void)
{
  PyObject *m;
  KvspoolType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&KvspoolType) < 0) return;
  m = Py_InitModule3("kvspool", NULL, "Kvspool module");
  Py_INCREF(&KvspoolType);
  PyModule_AddObject(m, "Kvspool", (PyObject *)&KvspoolType);
}

