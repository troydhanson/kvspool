OBJS=libts.a libzcontrol.a kvz zcon 
all: $(OBJS)
CFLAGS= -I. -I..
#CFLAGS+=-O2
CFLAGS+=-g 

ts.o: ts.c ts.h

libts.a: ts.o
	ar cr $@ $^

libzcontrol.a: zcontrol.o
	ar cr $@ $^

zcon: zcon.c 
	$(CC) $(CFLAGS) -c $<
	$(CC) $(CFLAGS) -o $@ $< -lzmq -lreadline 

kvz: kvz.c kvz_cmds.c libts.a libzcontrol.a
	$(CC) $(CFLAGS) -pthread -c kvz.c
	$(CC) $(CFLAGS) -pthread -c kvz_cmds.c
	$(CC) $(CFLAGS) -pthread -o kvz kvz.o kvz_cmds.o -lzmq -lkvspool -L. -lts -lzcontrol

.PHONY: clean

clean:
	rm -f *.o $(OBJS)
