all: KVJava.class libKVJava.so
JNIINC=/usr/lib/jvm/java-6-openjdk/include/
JNILIB=/usr/lib/jvm/java-6-openjdk/jre/lib/i386/

KVJava.class: KVJava.java
	javac $<

libKVJava.so: KVJava.class KVJava.c
	javah -jni KVJava
	$(CC) -o $@ -fPIC -shared -I. -I${JNIINC} -I../include KVJava.c -L../src/ -L.. -lkvspool -L${JNILIB} -ljava -static-libgcc

.PHONY: clean

clean:
	rm -f KVJava.class KVJava.h
