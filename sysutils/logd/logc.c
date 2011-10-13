#include <syslog.h>
#include <stdio.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include "utstring.h"
#include "logc.h"
#include "logd.h"

/* the program that links with this library should define
 * the module name to something meaningful. it will cause 
 * a log file with that name to be written */
extern char *modname;

struct {
  char *logd_fifo;
  int logd_fd;
  int on_tty;
  int suppress_warning;
} cfg = {
  .logd_fifo = LOGD_FIFO,
  .logd_fd = -1,
};

int has_controlling_terminal(void) {
  int fd;
  fd = open("/dev/tty",O_RDONLY);
  if (fd == -1) return 0;
  close(fd);
  return 1;
}

#define LOGC_MSGMAX ((PIPE_BUF <= 0xffff) ? (PIPE_BUF-2) : (0xffff-2))
void write_to_logd(UT_string *s,int prefix_len) {
  uint16_t len = (uint16_t)utstring_len(s);
  char *msg = utstring_body(s);
  int rc,td;

  /* is the logd fifo open for writing? if not try to open it*/
  if (cfg.logd_fd == -1) {
    if (has_controlling_terminal) cfg.on_tty=1;
    cfg.logd_fd = open(cfg.logd_fifo,O_WRONLY|O_NONBLOCK);
    if (cfg.logd_fd == -1) {
      if (!cfg.suppress_warning) {
        syslog(LOG_LOCAL0|LOG_INFO,"logd not running"); 
        cfg.suppress_warning=1;
      }
      if (cfg.on_tty) fprintf(stderr, "%s\n", msg+prefix_len);
      syslog(LOG_LOCAL0|LOG_INFO,"%s", msg);
      return;
    }
  }
  if (cfg.on_tty) fprintf(stderr, "%s\n", msg+prefix_len);
  if (len > LOGC_MSGMAX) len = LOGC_MSGMAX; /* atomic write limit */
  memcpy(msg,&len,sizeof(uint16_t)); /* prepend message len (inclusive) */
  if ( (rc=write(cfg.logd_fd, msg, len)) != len) {
    syslog(LOG_LOCAL0|LOG_ERR,"write to logd failed (%s)", ((rc==-1) ?
           strerror(errno) : "partial write"));
    close(cfg.logd_fd);
    cfg.logd_fd = -1;
  }
}

void lm(char *fmt, ...) {
  va_list ap;
  va_start(ap,fmt);

  UT_string *s;
  utstring_new(s);

  utstring_printf(s,"..%s|", modname?modname:"");
  utstring_printf_va(s, fmt, ap);
  write_to_logd(s,strlen(modname)+3);

  utstring_free(s);
  va_end(ap);
}

