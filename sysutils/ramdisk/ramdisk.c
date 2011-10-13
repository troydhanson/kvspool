#include <sys/mount.h>
#include <syslog.h>
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
#include "utarray.h"

#define TMPFS_MAGIC           0x01021994
 
/******************************************************************************
 * ramdisk
 *   a utility with modes to: 
 *   - create a ramdisk,
 *   - attrition its contents (continually or once)
 *****************************************************************************/

/* command line configuration parameters */
int verbose;
enum {MODE_NONE,MODE_QUERY,MODE_CREATE,MODE_UNMOUNT} mode = MODE_NONE;
char *sz="50%";
char *ramdisk;
UT_array *dirs;
 
void usage(char *prog) {
  fprintf(stderr, "usage:\n\n");
  fprintf(stderr, "Create ramdisk, unless it already exists:\n");
  fprintf(stderr, "   %%%s -c [-s 10g] [-d dir [-d dir]] /path/to/ramdisk\n", prog);
  fprintf(stderr, "   size is bytes, or suffixed with k|m|g|%% [def: 50%%]\n");
  fprintf(stderr, "   [-d dir] are dirs in ramdisk to create (absolute path)\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "Query mount point to detect/describe ramdisk\n");
  fprintf(stderr, "   %%%s -q /path/to/ramdisk\n", prog);
  fprintf(stderr, "   /proc/mounts lists all the ramdisks (and other disks)\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "Unmount ramdisk\n");
  fprintf(stderr, "   %%%s -u /path/to/ramdisk\n", prog);
  fprintf(stderr, "\n");
  exit(-1);
}

int suitable_mountpoint(char *dir, struct stat *sb, struct statfs *sf) {
  if (stat(ramdisk, sb) == -1) { /* does mount point exist? */
    syslog(LOG_ERR, "no mount point %s: %s\n", ramdisk, strerror(errno));
    return -1;
  }
  if (S_ISDIR(sb->st_mode) == 0) { /* has to be a directory */
    syslog(LOG_ERR, "mount point %s: not a directory\n", ramdisk);
    return -1;
  }
  if (statfs(ramdisk, sf) == -1) { /* what kinda filesystem is it on? */
    syslog(LOG_ERR, "can't statfs %s: %s\n", ramdisk, strerror(errno));
    return -1;
  }
  return 0;
}

#define KB 1024L
#define MB (1024*1024L)
#define GB (1024*1024*1024L)
int query_ramdisk(void) {
  struct stat sb; struct statfs sf;
  if (suitable_mountpoint(ramdisk, &sb, &sf) == -1) return -1;
  if (sf.f_type != TMPFS_MAGIC) {
    printf("%s: not a ramdisk\n", ramdisk);
    return -1;
  }
  char szb[100];
  long bytes = sf.f_bsize*sf.f_blocks;
  if (bytes < KB) snprintf(szb, sizeof(szb), "%ld bytes", bytes);
  else if (bytes < MB) snprintf(szb, sizeof(szb), "%ld kb", bytes/KB);
  else if (bytes < GB) snprintf(szb, sizeof(szb), "%ld mb", bytes/MB);
  else                 snprintf(szb, sizeof(szb), "%ld gb", bytes/GB);
  int used_pct = 100 - (sf.f_bfree * 100.0 / sf.f_blocks);
  printf("%s: ramdisk of size %s (%d%% used)\n", ramdisk, szb, used_pct);
}

int unmount_ramdisk(void) {
  struct stat sb; struct statfs sf;
  if (suitable_mountpoint(ramdisk, &sb, &sf) == -1) return -1;
  if (sf.f_type != TMPFS_MAGIC) {
    syslog(LOG_ERR,"%s: not a ramdisk\n", ramdisk);
    return -1;
  }
  if (umount(ramdisk) == -1) {
    syslog(LOG_ERR,"%s: cannot unmount\n", ramdisk);
    return -1;
  }
  return 0;
}

int create_ramdisk(void) {
  int rc=-1;
  char opts[100];

  struct stat sb; struct statfs sf;
  if (suitable_mountpoint(ramdisk, &sb, &sf) == -1) goto done;
  if (sf.f_type == TMPFS_MAGIC) { rc=1; goto done; } /* already a ramdisk? */

  /* ok, mount a ramdisk on this point */
  snprintf(opts,sizeof(opts),"size=%s",sz);
  if ( (rc=mount("unused", ramdisk, "tmpfs", MS_NOATIME|MS_NODEV, opts))==-1) {
    syslog(LOG_ERR, "can't make ramdisk %s: %s\n", ramdisk, strerror(errno));
  }

 done:
  if (rc == 0) syslog(LOG_INFO,"mounted ramdisk %s (size %s)\n", ramdisk, sz);
  else if (rc == 1) syslog(LOG_INFO,"ramdisk %s exists\n", ramdisk);
  else if (rc == -1) syslog(LOG_ERR,"failed to make ramdisk %s\n", ramdisk);
  return rc;
}

void make_dirs(UT_array *dirs) {
  char **d, *dir;
  d=NULL;
  while ( (d=(char**)utarray_next(dirs,d))) {
    dir = *d;
    /* fprintf(stderr,"dir is %s\n",dir); */
    if (mkdir(dir, 0777) == -1) {
      fprintf(stderr,"failed to make %s: %s\n",dir,strerror(errno));
    }
  }
}

int main(int argc, char * argv[]) {
  int opt, rc;
  utarray_new(dirs,&ut_str_icd);
 
  while ( (opt = getopt(argc, argv, "v+cqus:hd:")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'q': if (mode) usage(argv[0]); mode=MODE_QUERY; break;
      case 'c': if (mode) usage(argv[0]); mode=MODE_CREATE; break;
      case 'u': if (mode) usage(argv[0]); mode=MODE_UNMOUNT; break;
      case 's': sz=strdup(optarg); break;
      case 'd': utarray_push_back(dirs,&optarg); break;
      case 'h': default: usage(argv[0]); break;
    }
  }
  if (optind < argc) ramdisk=argv[optind++];
  if (!ramdisk) usage(argv[0]);
  openlog("ramdisk", LOG_PID|LOG_PERROR, LOG_LOCAL0);

  switch(mode) {
    case MODE_CREATE: rc=create_ramdisk(); make_dirs(dirs); break;
    case MODE_UNMOUNT: rc=unmount_ramdisk(); break;
    case MODE_QUERY: rc=query_ramdisk(); break;
    default: usage(argv[0]); break;
  }
  utarray_free(dirs);
  return rc;
}
