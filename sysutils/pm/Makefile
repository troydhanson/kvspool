SUBDIRS=lemon
all: $(SUBDIRS) pm
CFLAGS=-I /usr/local/include
CFLAGS+=-g


.PHONY: clean $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@

cfg.c: cfg.y
	lemon/lemon $<

pm: pm.c cfg.c job.c tok.c
	$(CC) $(CFLAGS) -o $@ $^

clean:
	rm -f *.o *.out pm cfg.c cfg.h
	for f in $(SUBDIRS); do make -C $$f $@; done

install: pm
	cp $^ /usr/local/bin
	cp upstart/pm.conf /etc/init/pm.conf
	touch /etc/pm.conf
