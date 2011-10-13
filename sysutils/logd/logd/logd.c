#include <sys/types.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <assert.h>
#include <dirent.h>
#include <syslog.h>
#include <string.h>
#include <limits.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include "logd.h"
#include "uthash.h"

#define DATETIME_FORMAT "%b %d %T " 
#define DATETIME_MAXLEN 50 
#define LOGD_MAXSZ (1024 * 1024) /* 1 mb */
#define MAINTENANCE_INTERVAL 10000 /* check log size every n lines */

struct {
  char *log_dir;
  int nmsgs;
  int verbose;
} cfg = {
  .log_dir = "/var/log/local",
};

/* we keep track of open log files with these */
typedef struct {
  int fd;
  char *src;
  int srclen;
  UT_hash_handle hh;
} logfile_t;

logfile_t *files = NULL;

char buf[PIPE_BUF];
char now[DATETIME_MAXLEN];

/* log messages are written to the fifo like "source|message". */
int parse_msg(int len, char **src, int *srclen, char **msg, int *msglen) {
  char *pipe = memchr(buf,'|',len);
  if (!pipe) return -1;

  *src = buf;
  *srclen = pipe-buf;
  //*pipe = '\0';
  *msg = pipe+1;
  *msglen = len - ((*msg)-buf);
  return 0;
}

int format_time(void) {
  struct timeval now_tv;
  time_t now_time;
  struct tm *tm;
  gettimeofday( &now_tv, NULL );  
  now_time = (time_t)now_tv.tv_sec;
  tm = localtime( &now_time );  
  return strftime(now, DATETIME_MAXLEN, DATETIME_FORMAT, tm);
}

int get_file(char *src,int srclen) {

  logfile_t *f;
  HASH_FIND(hh, files, src, srclen, f);
  if (f) return f->fd;
  //printf("cache miss\n");

  int fd = -1;
  char *path;
  UT_string *s;
  utstring_new(s);
  utstring_printf(s,"%s/%.*s.log", cfg.log_dir, srclen, src);
  path = utstring_body(s);

  fd = open(path,O_WRONLY|O_APPEND|O_CREAT, 0644);
  if (fd == -1) {
    syslog(LOG_ERR,"can't open %s: %s", path, strerror(errno));
  } else {
    f = malloc(sizeof(*f));
    f->src = strndup(src,srclen);
    f->srclen = srclen;
    f->fd = fd;
    HASH_ADD_KEYPTR(hh, files, f->src, f->srclen, f);
  }

  utstring_free(s);
  return fd;
}

void do_log(int len) {
  char *src, *msg, *m;
  int srclen, msglen, dtlen, fd;

  if (parse_msg(len,&src,&srclen,&msg,&msglen) == -1) {
    syslog(LOG_ERR,"malformed message received");
    return;
  }

  if ( (fd = get_file(src,srclen)) == -1) {
    syslog(LOG_INFO,"%.*s",len,buf);
    return;
  }

  dtlen = format_time();
  m = msg;
  while(m-msg < msglen) {
    if (*m == '\n') {
      write(fd, now, dtlen);
      write(fd, msg, m-msg+1);
      msglen -= (m-msg+1);
      msg = m+1;
    } else if (m-msg == msglen-1) {
      write(fd, now, dtlen);
      write(fd, msg, msglen);
      write(fd, "\n", 1);
    }
    m++;
  }
}

/* check if any open log(s) need to be rolled and roll them */
void roll_log(void) {
  struct stat sb;
  UT_string *s=NULL, *t=NULL;
  char *oldpath, *newpath, *cmd;
  logfile_t *f,*tmp;

  HASH_ITER(hh, files, f, tmp) {
    if ((fstat(f->fd, &sb) == 0) && (sb.st_size < LOGD_MAXSZ)) continue;
        
    utstring_renew(s);
    utstring_renew(t);

    /* rotate x.log to x.log.0. we only keep one rotated log for now */
    utstring_printf(s, "%s/%s.log",   cfg.log_dir, f->src);
    utstring_printf(t, "%s/%s.log.0", cfg.log_dir, f->src);
    oldpath = utstring_body(s);
    newpath = utstring_body(t);
    unlink(newpath); /* blow away old .0, if it exists */
    if (link(oldpath, newpath) == -1) {
      syslog(LOG_ERR, "link %s failed: %s", newpath, strerror(errno));
    }
    if (unlink(oldpath) == -1) {
      syslog(LOG_ERR, "unlink %s failed: %s", oldpath, strerror(errno));
    }

    HASH_DEL(files,f);
    close(f->fd);
    free(f->src);
    free(f);
  }
  if (s) utstring_free(s);
  if (t) utstring_free(t);
}

void usage(char *prog) {
  fprintf(stderr, "usage: %s [-d <logdir>] [-v]\n", prog);
  exit(-1);
}
 
int main(int argc, char *argv[]) {
  int opt, rc, fd=-1;
  uint16_t msglen, bytes_read, bytes_left;
  DIR *d;

  openlog("logd",LOG_PID,LOG_LOCAL0);

  while ( (opt = getopt(argc, argv, "d:v+")) != -1) {
    switch (opt) {
      case 'v': cfg.verbose++; break;
      case 'd': cfg.log_dir = strdup(optarg); break;
      default: usage(argv[0]); break;
    }
  }

  /* ensure the logs directory exists */
  if ( (!(d=opendir(cfg.log_dir))) && (mkdir(cfg.log_dir, 0755) == -1)) {
    syslog(LOG_ERR, "can't create %s: %s", cfg.log_dir, strerror(errno));
    exit(-1);
  }
  closedir(d);

  /* create the fifo, ok if it already exists */
  rc = mkfifo(LOGD_FIFO, 0622);
  if ((rc == -1) && (errno != EEXIST)) {
    syslog(LOG_ERR, "can't create %s: %s", LOGD_FIFO, strerror(errno));
    exit(-1);
  }

  /* open the fifo for reading */
 reopen:
  if (fd != -1) close(fd);
  fd = open(LOGD_FIFO, O_RDONLY);
  if (fd == -1) {
    syslog(LOG_ERR, "can't open %s: %s", LOGD_FIFO, strerror(errno));
    exit(-1);
  }

  while (1) {
    rc = read(fd, &msglen, sizeof(msglen));       /* read length prefix */
    if (rc == 0) goto reopen;  /* last writer closed fifo, wait for new */
    else if ((rc == -1) && (errno == EINTR)) continue;        /* signal */
    else if ((rc == -1) && (errno != EINTR)) {
        syslog(LOG_ERR, "can't read %s: %s", LOGD_FIFO, strerror(errno));
        goto reopen;
    } 
    else if (rc != sizeof(msglen)) goto reopen;/* can't read two bytes? */
    else if (rc == sizeof(msglen)) {
      if ((msglen < 2) || (msglen > sizeof(buf))) goto reopen;/* sanity */
      msglen -= sizeof(msglen); /* subtract prefix len from message len */
    }
    bytes_read = 0;
    bytes_left = msglen;
    while(bytes_left) {
      rc = read(fd, &buf[bytes_read], bytes_left);
      if (rc == 0) goto reopen; /* malformed, partial message then eof */
      else if (rc == -1 && errno != EINTR) {
        syslog(LOG_ERR, "can't read %s: %s", LOGD_FIFO, strerror(errno));
        goto reopen;
      } else if (rc > 0) { bytes_read += rc; bytes_left -= rc; }
    }

    if (bytes_read == 0) continue;
    do_log(bytes_read);
    if ((cfg.nmsgs++ % MAINTENANCE_INTERVAL) == 0) roll_log();
  }
}
