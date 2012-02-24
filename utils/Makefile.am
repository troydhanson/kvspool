AM_CFLAGS = -I.. -I../include
LIBSPOOL = -L.. -lkvspool
bin_PROGRAMS = kvsp-spr kvsp-spw kvsp-tee kvsp-size kvsp-status \
               kvsp-speed kvsp-mod kvsp-reset \
               ramdisk

kvsp_spr_LDADD = $(LIBSPOOL)
kvsp_spw_LDADD = $(LIBSPOOL)
kvsp_tee_LDADD = $(LIBSPOOL)
kvsp_size_LDADD = $(LIBSPOOL)
kvsp_status_LDADD = $(LIBSPOOL)
kvsp_speed_LDADD = $(LIBSPOOL)
kvsp_mod_LDADD = $(LIBSPOOL)
kvsp_reset_LDADD = $(LIBSPOOL)
kvsp_pub_LDADD = $(LIBSPOOL)
kvsp_sub_LDADD = $(LIBSPOOL)

if HAVE_ZEROMQ
if HAVE_JANSSON
bin_PROGRAMS += kvsp-sub kvsp-pub 
kvsp_pub_LDADD += -lzmq -ljansson
kvsp_sub_LDADD += -lzmq -ljansson
endif
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
