AM_CFLAGS = -I.. -I../include
LIBSPOOL = -L.. -lkvspool
bin_PROGRAMS = kvsp-spr kvsp-spw kvsp-tee kvsp-size kvsp-status \
               kvsp-speed kvsp-mod kvsp-reset \
               ramdisk

kvsp-spr_LDADD = $(LIBSPOOL)
kvsp-spw_LDADD = $(LIBSPOOL)
kvsp-tee_LDADD = $(LIBSPOOL)
kvsp-size_LDADD = $(LIBSPOOL)
kvsp-status_LDADD = $(LIBSPOOL)
kvsp-speed_LDADD = $(LIBSPOOL)
kvsp-mod_LDADD = $(LIBSPOOL)
kvsp-reset_LDADD = $(LIBSPOOL)

if HAVE_ZEROMQ
bin_PROGRAMS += kvsp-sub kvsp-pub
kvsp_pub_LDADD = $(LDADD) -lzmq
kvsp_sub_LDADD = $(LDADD) -lzmq
endif

# to get a rebuild of the utilities when ../libkvspool.a changes:
kvsp_spr_DEPENDENCIES = ../libkvspool.a
kvsp_spw_DEPENDENCIES = ../libkvspool.a
kvsp_tee_DEPENDENCIES = ../libkvspool.a
kvsp_size_DEPENDENCIES = ../libkvspool.a
kvsp_status_DEPENDENCIES = ../libkvspool.a
kvsp_speed_DEPENDENCIES = ../libkvspool.a
kvsp_mod_DEPENDENCIES = ../libkvspool.a
kvsp_reset_DEPENDENCIES = ../libkvspool.a
kvsp_sub_DEPENDENCIES = ../libkvspool.a
kvsp_pub_DEPENDENCIES = ../libkvspool.a
