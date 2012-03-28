#include <limits.h>
#include <jni.h>
#include "KVJava.h"
#include "kvspool.h"

typedef struct {
  void *spr;
  void *spw;
  void *set;
  char dir[PATH_MAX];
} kvsp_handle_t;

/* retrieve the handle from the object, creating it if need be */
static kvsp_handle_t *get_handle(JNIEnv *env, jobject obj) {
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID id = (*env)->GetFieldID(env, cls, "kvsp_handle", "J");
    jlong kvsp_handle_jlong = (*env)->GetLongField(env, obj, id);
    /* doing the cast through tmp silences a compiler warning on 32-bit */
    intptr_t tmp = (intptr_t)kvsp_handle_jlong;
    kvsp_handle_t *kvsp_handle = (kvsp_handle_t*)tmp;
    if (kvsp_handle == NULL) {
      kvsp_handle = calloc(1, sizeof(kvsp_handle_t));
      kvsp_handle->set = kv_set_new();
      (*env)->SetLongField(env, obj, id, (long)kvsp_handle);
    }
    return kvsp_handle;
}

static const char *get_dir(JNIEnv *env, jobject obj) {
    kvsp_handle_t *handle = get_handle(env, obj);
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID id = (*env)->GetFieldID(env, cls, "dir", "Ljava/lang/String;");
    jstring dirj = (jstring) (*env)->GetObjectField(env, obj, id);
    kvsp_handle_t *h = get_handle(env, obj);
    const char* dir = (*env)->GetStringUTFChars(env, dirj, 0);
    strncpy(h->dir, dir, sizeof(h->dir));
    (*env)->ReleaseStringUTFChars(env, dirj, dir);
    return h->dir;
}

static unsigned char get_blocking(JNIEnv *env, jobject obj) {
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID id = (*env)->GetFieldID(env, cls, "blocking", "Z");
    jboolean blockingj = (jboolean) (*env)->GetBooleanField(env, obj, id);
    unsigned char blocking = (unsigned char)blockingj;
    return blocking;
}

jint throwIoError(JNIEnv *env, char* message) {
    jclass exClass;
    char *className = "java/io/IOException";

    exClass = (*env)->FindClass(env, className);
    if (exClass == NULL) {
        //return throwNoClassDefError(env, className);
        return -1;
    }

    return (*env)->ThrowNew(env, exClass, message);
}

jobject kvs_to_map(JNIEnv* env, void *set) {
    jclass mapClass = NULL;
    static jmethodID init = NULL;
    static jmethodID put = NULL;
    //if (mapClass == NULL) {
    mapClass = (*env)->FindClass(env, "java/util/HashMap");
    //}
    if (mapClass == NULL) {
        return NULL;
    }

    jsize map_len = kv_len(set);
    if (init == NULL) {
        init = (*env)->GetMethodID(env, mapClass, "<init>", "(I)V");
    }
    jobject obj = (*env)->NewObject(env, mapClass, init, map_len);
    if (put == NULL) {
        put = (*env)->GetMethodID(env, mapClass, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    }

    kv_t *kv = NULL;
    while ((kv = kv_next(set, kv))) {
        jstring key = (*env)->NewStringUTF(env, kv->key);
        jstring val = (*env)->NewStringUTF(env, kv->val);
        (*env)->CallObjectMethod(env, obj, put, key, val);
    }

    return obj;
}

JNIEXPORT jobject JNICALL Java_KVJava_read(JNIEnv * env, jobject obj) {
  kvsp_handle_t *h = get_handle(env, obj);
  jobject ret=NULL;

  if (h->spr == NULL) {
    const char *dir = get_dir(env, obj);
    h->spr = kv_spoolreader_new(dir);
    if (h->spr == NULL) {
      throwIoError(env,"Failed to open spool");
      return NULL;
    }
  }
  int blocking = get_blocking(env, obj);
  int rc = kv_spool_read(h->spr, h->set, blocking);
  if (rc < 0) {
      throwIoError(env,"Spool reader error");
  } else if (rc == 0) {
    // TODO non blocking support. throw something or what?
  } else {
     ret = kvs_to_map(env, h->set);
  }
  return ret;
}

int map_to_kvs(JNIEnv* env, jobject obj, void *set) {
    jclass mapClass = NULL;
    jclass setClass = NULL;
    jclass iteratorClass = NULL;
    static jmethodID get = NULL;

    static jmethodID keySet = NULL;
    static jmethodID iterator = NULL;
    static jmethodID hasNext = NULL;
    static jmethodID next = NULL;
    //if (mapClass == NULL) {
    mapClass = (*env)->FindClass(env, "java/util/HashMap");

    setClass = (*env)->FindClass(env, "java/util/Set");
    iteratorClass = (*env)->FindClass(env, "java/util/Iterator");
    //}

    if (get == NULL) {
        get = (*env)->GetMethodID(env, mapClass, "get", "(Ljava/lang/Object;)Ljava/lang/Object;");
    }
    if (keySet == NULL) {
        keySet = (*env)->GetMethodID(env, mapClass, "keySet", "()Ljava/util/Set;");
    }
    if (iterator == NULL) {
        iterator = (*env)->GetMethodID(env, setClass, "iterator", "()Ljava/util/Iterator;");
    }
    if (hasNext == NULL) {
        hasNext = (*env)->GetMethodID(env, iteratorClass, "hasNext", "()Z");
    }
    if (next == NULL) {
        next = (*env)->GetMethodID(env, iteratorClass, "next", "()Ljava/lang/Object;");
    }
    jobject sets = (*env)->CallObjectMethod(env, obj, keySet);
    jobject iter = (*env)->CallObjectMethod(env, sets, iterator);
    jmethodID toStringK = NULL;
    jmethodID toStringV = NULL;
    jclass cls = NULL;
    while ((*env)->CallBooleanMethod(env, iter, hasNext)) {


        jobject key = (*env)->CallObjectMethod(env, iter, next);
        jobject val = (*env)->CallObjectMethod(env, obj, get, key);
        cls = (*env)->GetObjectClass(env, key);
        toStringK = (*env)->GetMethodID(env, cls, "toString", "()Ljava/lang/String;");
        key = (*env)->CallObjectMethod(env, key, toStringK);
        cls = (*env)->GetObjectClass(env, val);
        toStringV = (*env)->GetMethodID(env, cls, "toString", "()Ljava/lang/String;");
        val = (*env)->CallObjectMethod(env, val, toStringV);
        const char* keyc = (*env)->GetStringUTFChars(env, key, 0);

        const char* valc = (*env)->GetStringUTFChars(env, val, 0);
        kv_add(set, keyc, (*env)->GetStringUTFLength(env, key), valc, (*env)->GetStringUTFLength(env, val));

        (*env)->ReleaseStringUTFChars(env, key, keyc);

        (*env)->ReleaseStringUTFChars(env, val, valc);
    }

    //return obj;
    return -1;
}

JNIEXPORT void JNICALL Java_KVJava_write(JNIEnv *env, jobject obj, jobject map) {
  kvsp_handle_t *h = get_handle(env, obj);
  if (h->spw == NULL) {
    const char *dir = get_dir(env, obj);
    h->spw = kv_spoolwriter_new(dir);
    if (h->spw == NULL) {
      throwIoError(env,"Failed to open spool");
      return;
    }
  }
  map_to_kvs(env, map, h->set);
  kv_spool_write(h->spw, h->set);
}

JNIEXPORT void JNICALL Java_KVJava_close(JNIEnv *env, jobject obj) {
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID kvsp_handle_id = (*env)->GetFieldID(env, cls, "kvsp_handle", "J");
    jlong kvsp_handle_jlong = (*env)->GetLongField(env, obj, kvsp_handle_id);
    /* doing the cast through tmp silences a compiler warning on 32-bit */
    intptr_t tmp = (intptr_t)kvsp_handle_jlong;
    kvsp_handle_t *kvsp_handle = (kvsp_handle_t*)tmp;

    if (kvsp_handle == NULL) return;
    if (kvsp_handle->set) kv_set_free(kvsp_handle->set);
    if (kvsp_handle->spr) kv_spoolreader_free(kvsp_handle->spr);
    if (kvsp_handle->spw) kv_spoolwriter_free(kvsp_handle->spw);
    (*env)->SetLongField(env, obj, kvsp_handle_id, 0);
}
