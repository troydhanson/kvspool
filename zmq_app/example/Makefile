OBJS=libtracker.a kvzuri 
all: $(OBJS)
CFLAGS= -I. -I.. -I../..
#CFLAGS+=-O2
CFLAGS+=-g 

tracker.o: tracker.c tracker.h
	$(CC) $(CFLAGS) -c $<

libtracker.a: tracker.o
	ar cr $@ $^

kvzuri: kvzuri.c kvzuri_cmds.c ../libts.a libtracker.a ../libzcontrol.a
	$(CC) $(CFLAGS) -pthread -c kvzuri.c
	$(CC) $(CFLAGS) -pthread -c kvzuri_cmds.c
	$(CC) $(CFLAGS) -pthread -o kvzuri kvzuri.o kvzuri_cmds.o -lzmq -lkvspool -L.. -lts -lzcontrol -L. -ltracker 

.PHONY: clean

clean:
	rm -f *.o $(OBJS)
