AC_PREREQ(2.59)

AC_INIT([kvspool], [1.0], [tdh@tkhanson.net])
AC_CONFIG_SRCDIR(src/kvspool.c)
AC_CONFIG_AUX_DIR(config)
AC_CONFIG_HEADERS(config/config.h)
AM_INIT_AUTOMAKE([foreign subdir-objects])
AC_PROG_CC
AC_PROG_RANLIB

# is Jansson installed
AC_CHECK_LIB(jansson,json_string,
  AM_CONDITIONAL(HAVE_JANSSON,true),
  AM_CONDITIONAL(HAVE_JANSSON,false))

# is nanomsg installed
AC_CHECK_LIB(nanomsg,nn_socket,
  AM_CONDITIONAL(HAVE_NANOMSG,true),
  AM_CONDITIONAL(HAVE_NANOMSG,false))

# is zeromq (0MQ) installed
AC_CHECK_LIB(zmq,zmq_init,
  AM_CONDITIONAL(HAVE_ZEROMQ,true),
  AM_CONDITIONAL(HAVE_ZEROMQ,false))

# is libpcre3 installed
AC_CHECK_LIB(pcre,pcre_exec,
  AM_CONDITIONAL(HAVE_PCRE,true),
  AM_CONDITIONAL(HAVE_PCRE,false))

# is librdkafka installed
AC_CHECK_LIB(rdkafka,rd_kafka_new,
  AM_CONDITIONAL(HAVE_RDKAFKA,true),
  AM_CONDITIONAL(HAVE_RDKAFKA,false))

# is SSL installed. Not sure why Python build requires
AC_CHECK_LIB(ssl,SSL_library_init,
  AM_CONDITIONAL(HAVE_SSL,true),
  AM_CONDITIONAL(HAVE_SSL,false))

AX_PYTHON_DEVEL(>= '2.4')
AM_CONDITIONAL(HAVE_PYTHON,test "x$pythonexists" = "xyes") 

AC_CHECK_PROG(PERL,perl,perl)
AX_PROG_PERL_VERSION([5.10.1],
  AM_CONDITIONAL(HAVE_PERL,test "x$PERL" != "x"),
  AM_CONDITIONAL(HAVE_PERL,false))

AC_CONFIG_FILES(Makefile src/Makefile utils/Makefile)
AC_OUTPUT

