srcdir = @srcdir@

AM_CFLAGS = -I$(srcdir)/.. -I$(srcdir)/../include
LIBSPOOL = -L../src -lkvspool
bin_PROGRAMS = kvsp-spr kvsp-spw kvsp-init kvsp-status \
               kvsp-speed kvsp-mod kvsp-rewind kvsp-tpub \
               ramdisk kvsp-bcat

kvsp_spr_LDADD = $(LIBSPOOL)
kvsp_spw_LDADD = $(LIBSPOOL)
kvsp_tee_LDADD = $(LIBSPOOL)
kvsp_init_LDADD = $(LIBSPOOL)
kvsp_status_LDADD = $(LIBSPOOL)
kvsp_speed_LDADD = $(LIBSPOOL)
kvsp_mod_LDADD = $(LIBSPOOL)
kvsp_rewind_LDADD = $(LIBSPOOL)
kvsp_bcat_LDADD = $(LIBSPOOL)
kvsp_bpub_LDADD = $(LIBSPOOL)
kvsp_bsub_LDADD = $(LIBSPOOL)
kvsp_npub_LDADD = $(LIBSPOOL)
kvsp_nsub_LDADD = $(LIBSPOOL)
kvsp_pub_LDADD = $(LIBSPOOL)
kvsp_sub_LDADD = $(LIBSPOOL)
kvsp_concen_LDADD = $(LIBSPOOL)
kvsp_tpub_LDADD = $(LIBSPOOL)
kvsp_upub_LDADD = $(LIBSPOOL)
kvsp_kpub_LDADD = $(LIBSPOOL)

kvsp_bcat_SOURCES = kvsp-bcat.c kvsp-bconfig.c
kvsp_bpub_SOURCES = kvsp-bpub.c kvsp-bconfig.c
kvsp_bsub_SOURCES = kvsp-bsub.c kvsp-bconfig.c
kvsp_npub_SOURCES = kvsp-npub.c kvsp-bconfig.c
kvsp_nsub_SOURCES = kvsp-nsub.c kvsp-bconfig.c
kvsp_tpub_SOURCES = kvsp-tpub.c kvsp-bconfig.c

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
bin_PROGRAMS += kvsp-kpub
kvsp_kpub_LDADD += -lrdkafka -ljansson
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
kvsp_upub_LDADD += -lzmq -ljansson
kvsp_sub_LDADD += -lzmq -ljansson
kvsp_concen_LDADD += -lzmq -ljansson
endif
endif
if HAVE_JANSSON
bin_PROGRAMS += kvsp-upub
kvsp_upub_LDADD += -ljansson
endif

# to get a rebuild of the utilities when ../libkvspool.a changes:
kvsp_spr_DEPENDENCIES = ../src/libkvspool.a
kvsp_spw_DEPENDENCIES = ../src/libkvspool.a
kvsp_tee_DEPENDENCIES = ../src/libkvspool.a
kvsp_init_DEPENDENCIES = ../src/libkvspool.a
kvsp_status_DEPENDENCIES = ../src/libkvspool.a
kvsp_speed_DEPENDENCIES = ../src/libkvspool.a
kvsp_mod_DEPENDENCIES = ../src/libkvspool.a
kvsp_rewind_DEPENDENCIES = ../src/libkvspool.a
kvsp_sub_DEPENDENCIES = ../src/libkvspool.a
kvsp_pub_DEPENDENCIES = ../src/libkvspool.a
kvsp_bcat_DEPENDENCIES = ../src/libkvspool.a
kvsp_bpub_DEPENDENCIES = ../src/libkvspool.a
kvsp_bsub_DEPENDENCIES = ../src/libkvspool.a
kvsp_npub_DEPENDENCIES = ../src/libkvspool.a
kvsp_nsub_DEPENDENCIES = ../src/libkvspool.a
kvsp_concen_DEPENDENCIES = ../src/libkvspool.a
kvsp_tpub_DEPENDENCIES = ../src/libkvspool.a
kvsp_upub_DEPENDENCIES = ../src/libkvspool.a
