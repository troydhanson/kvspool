all: libts.a
CFLAGS=-g

ts.o: ts.c ts.h

libts.a: ts.o
	ar cr $@ $^

.PHONY: clean

clean:
	rm -f *.o libts.a
