CFLAGS=-I..
CFLAGS += -g

BIN=logd
all: $(BIN)

HDRS = logd.h 
SRCS = logd.c 
OBJS = $(patsubst %.c,%.o,$(SRCS))

logd: $(SRCS) $(HDRS) 
	$(CC) $(CFLAGS) -o $@ $(SRCS)

install: $(BIN)
	cp $(BIN) /usr/local/bin

clean:
	rm -f logd *.o
