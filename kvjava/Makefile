all: KVJava.class libKVJava.so
#CFLAGS = -g -O0
JNIINC=/usr/lib/jvm/java-6-openjdk/include/
JNILIB=/usr/lib/jvm/java-6-openjdk/jre/lib/i386/

KVJava.class: KVJava.java
	javac $<

KVJava.h: KVJava.class 
	javah -jni KVJava

libKVJava.so: KVJava.h KVJava.class KVJava.c
	$(CC) $(CFLAGS) -o $@ -fPIC -shared -I. -I${JNIINC} -I../include KVJava.c -L../src/ -L.. -lkvspool -L${JNILIB} -ljava -static-libgcc

.PHONY: clean test

test: 
	mkdir -p /tmp/spool
	javac TestKVJava.java
	java TestKVJava

clean:
	rm -f KVJava.h *.class *.so
