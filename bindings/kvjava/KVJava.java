
/* the KVJava object has methods implemented as JNI. This file is the Java
 * part of the class, which is compiled as usual with javac, then a
 * C header is generated from it (using javah -jni) for the JNI functions.
 * The implementation of the JNI functions in C is in KVJava.c. This gets
 * built into a shared library (which must be installed to a place the
 * dynamic linker can find it at runtime).
 */ 

import java.util.HashMap;
import java.lang.String;

public class KVJava {
  static {
    System.loadLibrary("KVJava"); // dynamically load native libKVJava.so
  }

  public boolean blocking;

  private String dir;       /* the spool directory */
  private long kvsp_handle; /* a C structure address, opaque to Java */

  public KVJava(String _dir) {
    dir = _dir;
    blocking = true;
  }

  public native HashMap<String,String> read();
  public native void write(HashMap<String,String> map);
  public native void close();
}
