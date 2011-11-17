#include <jni.h>
#include "KVJava.h"
#include "kvspool.h"

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

    jsize map_len = 10;
    if (init == NULL) {
        init = (*env)->GetMethodID(env, mapClass, "<init>", "(I)V");
    }
    jobject obj = (*env)->NewObject(env, mapClass, init, map_len);
    if (put == NULL) {
        put = (*env)->GetMethodID(env, mapClass, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    }

    kv_t *kv = NULL;
    while ((kv = kv_next(set, kv))) {
        //darn
        char* k = strndup(kv->key,kv->klen);
        jstring key = (*env)->NewStringUTF(env, k);
        free(k);
        char* v = strndup(kv->val,kv->vlen);
        jstring val = (*env)->NewStringUTF(env, v);
        free(v);
        (*env)->CallObjectMethod(env, obj, put, key, val);
    }

    return obj;
}

JNIEXPORT jobject JNICALL Java_KVJava_getFrame(JNIEnv * env, jobject obj, jboolean arg) {
    jclass cls = (*env)->GetObjectClass(env, obj);
    static jfieldID fid = NULL;
    static jfieldID basef = NULL;
    static jfieldID dirf = NULL;
    if (fid == NULL) {
        fid = (*env)->GetFieldID(env, cls, "rsp", "J"); //J is for long, since L is literal
    }
    jlong x = (*env)->GetLongField(env, obj, fid);
    if (x == 0) {
        if (basef == NULL) {
            basef = (*env)->GetFieldID(env, cls, "base", "Ljava/lang/String;"); //J is for long, since L is literal
        }
        if (dirf == NULL) {
            dirf = (*env)->GetFieldID(env, cls, "dir", "Ljava/lang/String;");
        }

        jstring basej = (jstring) (*env)->GetObjectField(env, obj, basef);
        jstring dirj = (jstring) (*env)->GetObjectField(env, obj, dirf);
        const char* base = (*env)->GetStringUTFChars(env, basej, 0);

        const char* dir = (*env)->GetStringUTFChars(env, dirj, 0);

        void* sp = kv_spoolreader_new(dir, base);

        (*env)->ReleaseStringUTFChars(env, basej, base);

        (*env)->ReleaseStringUTFChars(env, dirj, dir);
        if (sp == NULL) {
            
            throwFileNotFound(env,"Failed to open spool");
            return NULL;
        }
        jlong y = (long) sp;
        (*env)->SetLongField(env, obj, fid, y);
        x = y;

        //initialize

    }
    void* set = kv_set_new();
    int rc;
    jobject ret = NULL;
    void* sp = x;
    int block = (arg ? 1 : 0);
    if ((rc = kv_spool_read(sp, set, block)) > 0) {
        ret = kvs_to_map(env, set);
    }

    //x = (*env)->GetLongField(obj,fid);

    kv_set_free(set);

    return ret; //ret;
}

jint throwFileNotFound(JNIEnv *env, char* message) {
    jclass exClass;
    char *className = "java/io/IOException";

    exClass = (*env)->FindClass(env, className);
    if (exClass == NULL) {
        //return throwNoClassDefError(env, className);
        return -1;
    }

    return (*env)->ThrowNew(env, exClass, message);

}

JNIEXPORT void JNICALL Java_KVJava_putFrame(JNIEnv *env, jobject obj, jobject frame) {
    jclass cls = (*env)->GetObjectClass(env, obj);
    static jfieldID fid = NULL;
    static jfieldID basef = NULL;
    static jfieldID dirf = NULL;
    if (fid == NULL) {
        fid = (*env)->GetFieldID(env, cls, "wsp", "J"); //J is for long, since L is literal
    }
    jlong x = (*env)->GetLongField(env, obj, fid);
    if (x == 0) {
        if (basef == NULL) {
            basef = (*env)->GetFieldID(env, cls, "base", "Ljava/lang/String;"); //J is for long, since L is literal
        }
        if (dirf == NULL) {
            dirf = (*env)->GetFieldID(env, cls, "dir", "Ljava/lang/String;");
        }

        jstring basej = (jstring) (*env)->GetObjectField(env, obj, basef);
        jstring dirj = (jstring) (*env)->GetObjectField(env, obj, dirf);
        const char* base = (*env)->GetStringUTFChars(env, basej, 0);

        const char* dir = (*env)->GetStringUTFChars(env, dirj, 0);

        void* sp = kv_spoolwriter_new(dir, base);

        (*env)->ReleaseStringUTFChars(env, basej, base);

        (*env)->ReleaseStringUTFChars(env, dirj, dir);
        if (sp == NULL) {
            throwFileNotFound(env,"Failed to open output spool");
            return;
        }
        jlong y = (long) sp;
        (*env)->SetLongField(env, obj, fid, y);
        x = y;

        //initialize

    }
    void* set = kv_set_new();
    int rc;
    //jobject ret = NULL;
    void* sp = x;
    map_to_kvs(env, frame, set);

    kv_spool_write(sp, set);

    kv_set_free(set);

    // return ret; //ret;


}

JNIEXPORT void JNICALL Java_KVJava_close(JNIEnv *env, jobject obj) {
    jclass cls = (*env)->GetObjectClass(env, obj);
    static jfieldID rspid = NULL;
    static jfieldID wspid = NULL;
    if (rspid == NULL) {
        rspid = (*env)->GetFieldID(env, cls, "rsp", "J"); //J is for long, since L is literal
    }
    if (wspid == NULL) {
        wspid = (*env)->GetFieldID(env, cls, "wsp", "J"); //J is for long, since L is literal
    }
    jlong rsp = (*env)->GetLongField(env, obj, rspid);
    void* sp;
    if (rsp != 0) {
        sp = rsp;
        printf("freeing rsp\n");
        kv_spoolreader_free(sp);
        (*env)->SetLongField(env, obj, rspid, 0);
    }

    jlong wsp = (*env)->GetLongField(env, obj, wspid);
    if (wsp != 0) {
        sp = wsp;

        printf("freeing wsp\n");
        kv_spoolwriter_free(sp);

        (*env)->SetLongField(env, obj, wspid, 0);
    }

}
