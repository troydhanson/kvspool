OBJS=ramdisk
all: $(OBJS)
CFLAGS=-g

.PHONY: clean 

clean:
	rm -f *.o $(OBJS)

install: $(OBJS)
	cp $(OBJS) /usr/local/bin
