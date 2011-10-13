AM_CFLAGS = -fPIC
lib_LIBRARIES = libkvspool.a
libkvspool_a_SOURCES = kvspool.c kvspoolw.c kvspoolr.c kvspool_attrition.c tpl.c
include_HEADERS = kvspool.h

SUBDIRS = . utils

if HAVE_PYTHON
SUBDIRS += kvpy
endif

if HAVE_PERL
SUBDIRS += kvperl
endif
