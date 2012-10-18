AM_CFLAGS = -I.. -I../include
LIBSPOOL = -L../src -lkvspool
bin_PROGRAMS = kvsp-spr kvsp-spw kvsp-tee kvsp-init kvsp-status \
               kvsp-speed kvsp-mod kvsp-rewind \
               ramdisk

kvsp_spr_LDADD = $(LIBSPOOL)
kvsp_spw_LDADD = $(LIBSPOOL)
kvsp_tee_LDADD = $(LIBSPOOL)
kvsp_init_LDADD = $(LIBSPOOL)
kvsp_status_LDADD = $(LIBSPOOL)
kvsp_speed_LDADD = $(LIBSPOOL)
kvsp_mod_LDADD = $(LIBSPOOL)
kvsp_rewind_LDADD = $(LIBSPOOL)
kvsp_cast_LDADD = $(LIBSPOOL)
kvsp_pub_LDADD = $(LIBSPOOL)
kvsp_sub_LDADD = $(LIBSPOOL)
kvsp_concen_LDADD = $(LIBSPOOL)

if HAVE_ZEROMQ
bin_PROGRAMS += kvsp-cast
kvsp_cast_LDADD += -lzmq 
if HAVE_JANSSON
bin_PROGRAMS += kvsp-sub kvsp-pub kvsp-concen
kvsp_pub_LDADD += -lzmq -ljansson
kvsp_sub_LDADD += -lzmq -ljansson
kvsp_concen_LDADD += -lzmq -ljansson
endif
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
kvsp_cast_DEPENDENCIES = ../src/libkvspool.a
kvsp_concen_DEPENDENCIES = ../src/libkvspool.a
