SRCS = $(wildcard test*.c) 
PROGS = $(patsubst %.c,%,$(SRCS))

LIBDIR = ..
LIB = $(LIBDIR)/libts.a

CFLAGS = -I$(LIBDIR) -fno-strict-aliasing
CFLAGS += -g
CFLAGS += -Wall 
CFLAGS += ${EXTRA_CFLAGS}

TEST_TARGET=run_tests
TESTS=./do_tests

all: $(PROGS) $(TEST_TARGET) 

$(PROGS): $(LIB) 
	$(CC) $(CFLAGS) -o $@ $(@).c $(LIB)

run_tests: $(PROGS)
	perl $(TESTS)

.PHONY: clean

clean:	
	rm -f $(PROGS) test*.out 
	rm -rf *.dSYM
