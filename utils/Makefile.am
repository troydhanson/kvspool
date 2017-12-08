srcdir = @srcdir@

AM_CFLAGS = -I$(srcdir)/.. -I$(srcdir)/../include
LIBSPOOL = -L../src -lkvspool -lshr
bin_PROGRAMS = kvsp-spr kvsp-spw kvsp-init kvsp-status \
               kvsp-speed kvsp-mod kvsp-rewind \
               ramdisk kvsp-bcat kvsp-bshr kvsp-tsub kvsp-tpub

kvsp_spr_LDADD = $(LIBSPOOL)
kvsp_spw_LDADD = $(LIBSPOOL)
kvsp_tee_LDADD = $(LIBSPOOL)
kvsp_init_LDADD = $(LIBSPOOL)
kvsp_status_LDADD = $(LIBSPOOL)
kvsp_speed_LDADD = $(LIBSPOOL)
kvsp_mod_LDADD = $(LIBSPOOL)
kvsp_rewind_LDADD = $(LIBSPOOL)
kvsp_bcat_LDADD = $(LIBSPOOL)
kvsp_bshr_LDADD = $(LIBSPOOL)
kvsp_tsub_LDADD = $(LIBSPOOL)
kvsp_tpub_LDADD = $(LIBSPOOL)
kvsp_bpub_LDADD = $(LIBSPOOL)
kvsp_bsub_LDADD = $(LIBSPOOL)
kvsp_npub_LDADD = $(LIBSPOOL)
kvsp_nsub_LDADD = $(LIBSPOOL)
kvsp_pub_LDADD = $(LIBSPOOL)
kvsp_sub_LDADD = $(LIBSPOOL)
kvsp_concen_LDADD = $(LIBSPOOL)
kvsp_upub_LDADD = $(LIBSPOOL)
kvsp_kkpub_LDADD = $(LIBSPOOL)

kvsp_bcat_SOURCES = kvsp-bcat.c kvsp-bconfig.c
kvsp_bshr_SOURCES = kvsp-bshr.c kvsp-bconfig.c
kvsp_tsub_SOURCES = kvsp-tsub.c kvsp-bconfig.c
kvsp_tpub_SOURCES = kvsp-tpub.c kvsp-bconfig.c ringbuf.c
kvsp_bpub_SOURCES = kvsp-bpub.c kvsp-bconfig.c
kvsp_bsub_SOURCES = kvsp-bsub.c kvsp-bconfig.c
kvsp_npub_SOURCES = kvsp-npub.c kvsp-bconfig.c
kvsp_nsub_SOURCES = kvsp-nsub.c kvsp-bconfig.c

if HAVE_PCRE
bin_PROGRAMS += kvsp-tee
kvsp_tee_LDADD += -lpcre
endif

if HAVE_NANOMSG
bin_PROGRAMS += kvsp-npub kvsp-nsub
kvsp_npub_LDADD += -lnanomsg 
kvsp_nsub_LDADD += -lnanomsg 
endif

if HAVE_RDKAFKA
if HAVE_JANSSON
if HAVE_NANOMSG
bin_PROGRAMS += kvsp-kkpub
kvsp_kkpub_SOURCES = kvsp-kkpub.c ts.c ts.h
kvsp_kkpub_CFLAGS = ${AM_CFLAGS} -pthread 
kvsp_kkpub_LDADD += -lrdkafka -ljansson -lnanomsg 
endif
endif
endif

if HAVE_ZEROMQ
bin_PROGRAMS += kvsp-bpub kvsp-bsub
kvsp_bpub_LDADD += -lzmq 
kvsp_bsub_LDADD += -lzmq 
endif

if HAVE_ZEROMQ 
if HAVE_JANSSON
bin_PROGRAMS += kvsp-sub kvsp-pub kvsp-concen
kvsp_pub_LDADD += -lzmq -ljansson
kvsp_sub_LDADD += -lzmq -ljansson
kvsp_concen_LDADD += -lzmq -ljansson
endif
endif
if HAVE_JANSSON
bin_PROGRAMS += kvsp-upub
kvsp_upub_LDADD += -ljansson
endif

