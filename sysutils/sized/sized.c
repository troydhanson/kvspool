#include <sys/mount.h>
#include <sys/epoll.h>
#include <syslog.h>
#include <sys/inotify.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "logc.h"
#include "utarray.h"
#include "utstring.h"

/******************************************************************************
 * sized
 *   a utility to keep a directory under a certain size by deleting old files
 *****************************************************************************/
const char *modname = "sized";
#define PERIODIC_SCAN_INTERVAL (10*1000) /* 10 sec, expressed in milliseconds */

/* command line configuration parameters */
struct {
  int verbose;
  int once;
  int continuous;
  int query;
  int dry_run;
  char *sz;
  long sz_bytes;
  char *dir;
} cf = {
  .sz="90%",
};

typedef struct { 
  int file_idx;
  struct stat sb;
} file_stat_t;

UT_icd stats_icd = {sizeof(file_stat_t), NULL, NULL, NULL};
static int attrition_sort(const void *_a, const void *_b) {
  file_stat_t *a = (file_stat_t*)_a;
  file_stat_t *b = (file_stat_t*)_b;
  /* oldest modify time sorts first */
  return a->sb.st_mtime - b->sb.st_mtime;
}

void usage(char *prog) {
  fprintf(stderr, "usage:\n\n");
  fprintf(stderr, "Delete files to keep dir under size x\n");
  fprintf(stderr, "   %s -o|-c [-vd] [-s 10g] dir\n", prog);
  fprintf(stderr, "   size is bytes, or suffixed with k|m|g|%% [def: 90%% of fs]\n");
  fprintf(stderr, "   -o (once) run just once\n");
  fprintf(stderr, "   -c (continuous) run indefinitely to maintain size\n");
  fprintf(stderr, "   -d (dry run) do not remove any files, only print\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "Query directory size\n");
  fprintf(stderr, "   %s -q dir\n", prog);
  fprintf(stderr, "\n");
  fprintf(stderr, "Note: recursion is not supported\n");
  fprintf(stderr, "\n");
  exit(-1);
}

int get_files(UT_array *files, UT_array *stats) {
  struct dirent *dent;
  char *name, *path;
  file_stat_t fsb;
  int rc=-1,i=0;
  DIR *d;

  UT_string *s;
  utstring_new(s);
  utarray_clear(files);
  utarray_clear(stats);

  if ( (d = opendir(cf.dir)) == NULL) {
    lm("failed to opendir [%s]: %s\n", cf.dir, strerror(errno));
    goto done;
  }

  while ( (dent = readdir(d)) != NULL) {
    if (dent->d_type != DT_REG) continue;
    if (dent->d_name[0] == '.') continue; /* skip dot files */
    // ok, fully qualify it and push it 
    utstring_clear(s);
    utstring_printf(s, "%s/%s", cf.dir, dent->d_name);
    path = utstring_body(s);
    if (stat(path,&fsb.sb) == -1) {
      lm("can't stat %s: %s", path, strerror(errno));
      continue;
    }
    fsb.file_idx = i++;
    utarray_push_back(files, &path);
    utarray_push_back(stats, &fsb);
  }
  rc = 0; // success 

 done:
  utstring_free(s);
  if (d) closedir(d);
  return rc;
}

int do_attrition(void) {
  int rc = -1, nfiles=0;
  UT_array *files;
  UT_array *stats; 
  utarray_new(files,&ut_str_icd);
  utarray_new(stats,&stats_icd);
  if (get_files(files,stats) == -1) goto done;

  /* tally up their sizes */
  long total_sz=0;
  file_stat_t *fs=NULL;
  while ( (fs=(file_stat_t*)utarray_next(stats,fs))) total_sz += fs->sb.st_size;
  if (total_sz < cf.sz_bytes) { rc = 0; goto done; }

  /* we're oversize. sort the files oldest first and delete til under max size*/
  utarray_sort(stats,attrition_sort);
  fs=NULL;
  while ( (fs=(file_stat_t*)utarray_next(stats,fs))) {
    char *file = *(char**)utarray_eltptr(files, fs->file_idx);
    if (cf.verbose) lm("removing %s (size %ld)", file, (long)fs->sb.st_size);
    if (cf.dry_run || (unlink(file) == 0)) {
      total_sz -= fs->sb.st_size;
      nfiles++;
      if (total_sz < cf.sz_bytes) { rc = 0; goto done; }
    } else {
      lm("can't unlink %s: %s", file, strerror(errno));
    }
  }

 done:
  if (cf.verbose) lm("%d files removed", nfiles);
  utarray_free(files);
  utarray_free(stats);
  return rc;
}

#define KB 1024L
#define MB (1024*1024L)
#define GB (1024*1024*1024L)
char *hsz(long bytes, char *szb, int szl) {
  if (bytes < KB) snprintf(szb, szl, "%ld bytes", bytes);
  else if (bytes < MB) snprintf(szb, szl, "%ld kb", bytes/KB);
  else if (bytes < GB) snprintf(szb, szl, "%ld mb", bytes/MB);
  else                 snprintf(szb, szl, "%ld gb", bytes/GB);
  return szb;
}

int do_query(void) {
  int rc = -1;
  UT_array *files;
  UT_array *stats; 
  utarray_new(files,&ut_str_icd);
  utarray_new(stats,&stats_icd);
  if (get_files(files,stats) == -1) goto done;

  /* tally up their sizes */
  long total_sz=0;
  file_stat_t *fs=NULL;
  while ( (fs=(file_stat_t*)utarray_next(stats,fs))) total_sz += fs->sb.st_size;

  char tsz[100],csz[100];
  lm("directory size: %s (limit %s)", hsz(total_sz, tsz, sizeof(tsz)), 
                                      hsz(cf.sz_bytes, csz, sizeof(csz)));

  rc = 0;

 done:
  utarray_free(files);
  utarray_free(stats);
  return rc;
}

/* lookup the size of the filesystem underlying cf.dir and 
 * calculate pct% of that size */
long get_fs_pct(int pct) {
  assert(pct > 0 && pct < 100);
  struct statfs fb;
  if (statfs(cf.dir, &fb) == -1) {
    lm("can't statfs %s: %s", cf.dir, strerror(errno));
    return -1;
  }
  long fsz = fb.f_bsize * fb.f_blocks; /* filesystem size */
  long cap = (fsz*pct) * 0.01;
  //lm("fs size: %ld * %ld%% = cap %ld bytes", fsz, pct, cap);
  return cap;
}

/* convert something like "20%" or "20m" to bytes.
 * percentage means 'percent of filesystem size' */
long sztobytes(void) {
  long n; int l; char unit;
  if (sscanf(cf.sz,"%ld",&n) != 1) return -1; /* e.g. 20 from "20m" */
  l = strlen(cf.sz); unit = cf.sz[l-1];
  if (unit >= '0' && unit <= '9') return n; /* no unit suffix */
  switch(unit) {
    default: return -1; break;
    case '%': n = get_fs_pct(n); break;
    case 't': n *= 1024; /* fall through */
    case 'g': n *= 1024; /* fall through */
    case 'm': n *= 1024; /* fall through */
    case 'k': n *= 1024;
  }
  return n;
}

int clear_file_event(int fd) {
  struct inotify_event *ev, *nx;
  int rc;
  size_t sz;

  union {
    struct inotify_event ev;
    char buf[sizeof(struct inotify_event) + PATH_MAX];
  } eb;

  rc = read(fd, &eb.ev, sizeof(eb));
  if (rc == -1) {
    lm("inotify read failed: %s", strerror(errno));
    return -1;
  }
  for(ev = &eb.ev; rc > 0; ev = nx) {

    sz = sizeof(*ev) + ev->len;
    nx = (struct inotify_event*)((char*)ev + sz);
    rc -= sz;

    if (ev->mask & IN_UNMOUNT) {
      lm("%s unmounted... exiting", cf.dir);
      return -2;
    }
  }

  /* it's ok if we didn't completely clear it; epoll will notify us again */
  return 0;
}

int main(int argc, char * argv[]) {
  int opt, rc=0, ifd=-1,efd=-1,er,mask,wd;
 
  while ( (opt = getopt(argc, argv, "v+s:ocdqh")) != -1) {
    switch (opt) {
      case 'v': cf.verbose++; break;
      case 'q': cf.query=1; break;
      case 'c': cf.continuous=1; break;
      case 'o': cf.once=1; break;
      case 's': cf.sz=strdup(optarg); break;
      case 'd': cf.dry_run=1; break;
      case 'h': default: usage(argv[0]); break;
    }
  }
  if (optind < argc) cf.dir=argv[optind++];
  if (!cf.dir) usage(argv[0]);
  if (cf.query + cf.once + cf.continuous != 1) usage(argv[0]); /* exclusive */
  if ( (cf.sz_bytes=sztobytes()) == -1) usage(argv[0]);
  if (cf.query) { do_query(); goto done; }

  if ( (rc=do_attrition()) == -1) goto done;
  if (!cf.continuous) goto done;

  /* continuous mode */
  if ( (ifd = inotify_init()) == -1) {
    lm("inotify_init failed: %s", strerror(errno));
    goto done;
  }
  mask = IN_CLOSE_WRITE|IN_UNMOUNT;
  if ( (wd = inotify_add_watch(ifd, cf.dir, mask)) == -1) {
    lm("inotify_add_watch failed");
    goto done;
  }
  /* wait for file events or periodic attrition */
  if ( (efd = epoll_create(5)) == -1) {
    lm("epoll_create failed: %s", strerror(errno));
    goto done;
  }
  struct epoll_event ev = {.events = EPOLLIN, .data.fd=ifd};
  if (epoll_ctl(efd, EPOLL_CTL_ADD, ifd, &ev) == -1) {
    lm("epoll_ctl failed: %s", strerror(errno));
    goto done;
  }
  do {
    er = epoll_wait(efd, &ev, 1, PERIODIC_SCAN_INTERVAL);
    switch(er) {
      case -1: lm("epoll_wait error: %s", strerror(errno)); break;
      case 1: if (clear_file_event(ifd) < 0) goto done; break;
      case 0: /* got timeout */; break;
      default: assert(0); break;
    }
    do_attrition();
  } while(er != -1);


 done:
  if (ifd != -1) close(ifd);
  if (efd != -1) close(efd);
  return rc;
}
