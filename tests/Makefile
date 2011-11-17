SRCS = $(wildcard test*.c) 
PROGS = $(patsubst %.c,%,$(SRCS))

LIBDIR = ..
LIB = ../libkvspool.a
LDFLAGS = -L$(LIBDIR) -lkvspool

CFLAGS = -I$(LIBDIR)/include -I$(LIBDIR) -fno-strict-aliasing
CFLAGS += -g
CFLAGS += -Wall 
CFLAGS += ${EXTRA_CFLAGS}

TEST_TARGET=run_tests
TESTS=./do_tests

all: $(PROGS) $(TEST_TARGET) 

$(PROGS): $(LIB) utils.o
	$(CC) $(CFLAGS) -o $@ $(@).c utils.o $(LDFLAGS)

run_tests: $(PROGS)
	perl $(TESTS)

.PHONY: clean

clean:	
	rm -f $(PROGS) test*.out 
	rm -rf *.dSYM
