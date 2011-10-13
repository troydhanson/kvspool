OBJS=sized
all: $(OBJS)
CFLAGS=-I../logd -g
LIBS=-L../logd -llogc

sized: sized.c
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

.PHONY: clean 

clean:
	rm -f *.o $(OBJS)

install: $(OBJS)
	cp $(OBJS) /usr/local/bin
