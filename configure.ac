AC_PREREQ(2.59)

AC_INIT([kvspool], [1.0], [troy.hanson@jhuapl.edu])
AC_CONFIG_SRCDIR(kvspool.c)
AC_CONFIG_AUX_DIR(config)
AC_CONFIG_HEADERS(config/config.h)
AM_INIT_AUTOMAKE
AC_PROG_CC
AC_PROG_RANLIB

# is zeromq (0MQ) installed
AC_CHECK_LIB(zmq,zmq_init,
  AM_CONDITIONAL(HAVE_ZEROMQ,true),
  AM_CONDITIONAL(HAVE_ZEROMQ,false))

AX_PYTHON_DEVEL(>= '2.4')
AM_CONDITIONAL(HAVE_PYTHON,test "x$pythonexists" = "xyes") 

AC_CHECK_PROG(PERL,perl,perl)
AM_CONDITIONAL(HAVE_PERL,test "x$PERL" != "x") 

AC_CONFIG_FILES(Makefile utils/Makefile)
AC_OUTPUT

