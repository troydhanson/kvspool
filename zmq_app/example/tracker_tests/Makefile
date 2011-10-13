SRCS = $(wildcard test*.c) 
PROGS = $(patsubst %.c,%,$(SRCS))

LIBDIR = ..
LIB = ../libtracker.a
LDFLAGS = -L$(LIBDIR) -ltracker

CFLAGS = -I$(LIBDIR) -fno-strict-aliasing -I../../..
CFLAGS += -g
CFLAGS += -Wall 
CFLAGS += ${EXTRA_CFLAGS}

TEST_TARGET=run_tests
TESTS=./do_tests

all: $(PROGS) $(TEST_TARGET) 

$(PROGS): $(LIB) 
	$(CC) $(CFLAGS) -o $@ $(@).c $(LDFLAGS)

run_tests: $(PROGS)
	perl $(TESTS)

.PHONY: clean

clean:	
	rm -f $(PROGS) test*.out 
	rm -rf *.dSYM
