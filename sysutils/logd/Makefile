# the installed header and include files
LIB = liblogc.a
HDRS = logc.h

SUBDIRS = logd 
all: $(LIB) $(SUBDIRS)

CFLAGS = -I. -fPIC -Ilogd
CFLAGS += -g
#CFLAGS += -O2
SRCS = logc.c 
OBJS = $(patsubst %.c,%.o,$(SRCS))

$(LIB): $(SRCS) $(HDRS) 
	$(CC) $(CFLAGS) -c $(SRCS)
	ar cr $@ $(OBJS)


.PHONY: clean $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@

clean:
	rm -f $(OBJS) $(LIB)
	for f in $(SUBDIRS); do make -C $$f $@; done

install: $(LIB)
	cp $(LIB) /usr/local/lib
	cp $(HDRS) /usr/local/include
	for f in $(SUBDIRS); do make -C $$f $@; done
