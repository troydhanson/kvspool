CFLAGS=-I..
CFLAGS+=-g
LDFLAGS=-L..
LIBS=-llogc

all: test1 test2

test1: test1.c ../liblogc.a
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

test2: test2.c ../liblogc.a
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)
