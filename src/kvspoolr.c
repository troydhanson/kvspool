#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/inotify.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <dirent.h>
#include "kvspool.h"
#include "kvspool_internal.h"
#include "utstring.h"
#include "tpl.h"

#define INTERNAL_RESCAN_INTERVAL_SEC (1)

typedef struct {
  char *srpath; /* e.g. /tmp/myspool/spool.123456789.999-000.sr */
  char *sppath; /* e.g. /tmp/myspool/spool.123456789.999-000.sp */
  int retries;  /* to detect corrupt spool */
  int sp_fd;
  int sr_fd;
  UT_hash_handle sppath_hh;
  unsigned pos;
  time_t last;
} kv_spoolrec_t;

typedef struct {
  char *dir;
  kv_spoolrec_t *spools;
  struct inotify_event *ev;
  size_t ev_sz;
  /* for periodic rescan of spool directory for new files */
  int need_rescan;
  time_t last_scan; 
  /* scratch space */
  UT_string *tmp;
} kv_spoolr_t;


static void unlock_spool(kv_spoolr_t *sp, kv_spoolrec_t *r) {
  HASH_DELETE(sppath_hh,sp->spools,r);
  if (r->srpath) free(r->srpath);
  if (r->sppath) free(r->sppath);
  close(r->sr_fd);
  close(r->sp_fd);
  free(r);
}

static void validate_lock_spool(kv_spoolr_t *sp, char *path) {
  kv_spoolrec_t *r;
  struct stat sb;
  int plen, fd;

  plen = strlen(path);

  /* already locked on the spool? nothing to do. */
  HASH_FIND(sppath_hh, sp->spools, path, plen, r);
  if (r) return;

  /*
   * we have a new spool file to lock, if it checks out.  we need to be able to
   * lock the spool reader marker, open the spool file for reading, etc. 
   * we also want to ignore locking fully spent (max-sized) spools unless they
   * have any unconsumed data. spent spools remain on disk til writer unlinks
   * them to make room at its discretion.
   */

  char *sppath = strdup(path);
  char *srpath = strdup(path); srpath[plen-1] = 'r';

  if ( (r = malloc(sizeof(*r))) == NULL) sp_oom();
  memset(r,0,sizeof(*r));
  r->srpath = srpath;
  r->sppath = sppath;
  r->retries = 0;
  if ( (r->sp_fd = open(r->sppath, O_RDONLY)) == -1) goto fail;
  if ( (r->sr_fd = open(r->srpath, O_RDWR)) == -1) goto fail;
  if (flock(r->sr_fd, LOCK_EX|LOCK_NB) == -1) goto fail;
  if ((fstat(r->sr_fd, &sb) == -1) || (sb.st_size != sizeof(r->pos))) goto fail;
  if (read(r->sr_fd, &r->pos, sizeof(r->pos)) != sizeof(r->pos)) goto fail;
  if ((fstat(r->sp_fd, &sb) == -1) || (sb.st_size < r->pos)) goto fail;
  if ((r->pos > kv_spool_options.file_max) && (sb.st_size <= r->pos)) goto fail;
  HASH_ADD_KEYPTR(sppath_hh,sp->spools,r->sppath,plen,r);
  return;

 fail:
  if (r->srpath) free(r->srpath);
  if (r->sppath) free(r->sppath);
  if (r->sp_fd != -1) close(r->sp_fd);
  if (r->sr_fd != -1) close(r->sr_fd);
  free(r);
  return;
}

static int spool_sort(kv_spoolrec_t *a, kv_spoolrec_t *b) {
  char *ahyph = strrchr(a->sppath,'-'), *bhyph = strrchr(b->sppath,'-');
  assert(ahyph && bhyph);
  int c, alen=ahyph-a->sppath, blen=bhyph-b->sppath;
  int min = (alen < blen) ? alen : blen;
  if (alen != blen) return strncmp(a->sppath, b->sppath, min);
  /* identical up through trailing sequence number. numerically compare 
   * the sequence numbers (note 1 < 10 is not true using strcmp) */
  int aseq,bseq;
  c=sscanf(ahyph+1,"%u",&aseq); assert(c==1);
  c=sscanf(bhyph+1,"%u",&bseq); assert(c==1);
  return aseq-bseq;
}

/* lock the available input spools. this function can get called many times 
 * during a spool reader's lifetime as files in the spool dir get created. 
 * Even though inotify tells us exactly which files were modified, we just
 * rescan the whole spool directory, add new locks and update our state. */
static int rescan_spools(kv_spoolr_t *sp) {
  kv_spoolrec_t *r, *tmp;
  char *path, **p;
  int rc = -1;

  UT_array *files;
  utarray_new(files, &ut_str_icd);

  if (sp_readdir(sp->dir, ".sp", files) == -1) goto done;

  /* first lock all the spools that are listed in the directory */
  p = NULL;
  while ( (p = (char**)utarray_next(files,p))) {
    path = *p;
    validate_lock_spool(sp, path);
  }

  /* unlock any spools that aren't in the directory (were deleted) */
  HASH_ITER(sppath_hh, sp->spools, r, tmp) {
    p = utarray_find(files, &r->sppath, sp_strsort);
    if (!p) unlock_spool(sp,r);
  }

  sp->last_scan = time(NULL);
  sp->need_rescan = 0;
  rc = 0;
  HASH_SRT(sppath_hh, sp->spools, spool_sort);

 done:
  utarray_free(files);
  return rc;
}

static int update_rpos(kv_spoolr_t *sp, kv_spoolrec_t *r, size_t sz) {
  r->pos += sz;
  if ((lseek(r->sr_fd,0,SEEK_SET) == -1) ||
      (write(r->sr_fd,&r->pos,sizeof(r->pos)) != sizeof(r->pos))) {
    fprintf(stderr,"failed to update srmark: %s\n", strerror(errno));
    r->pos -= sz;
    sp->need_rescan=1;
    return -1;
  }
  return 0; /* success */
}

static void fill_set(void *img, size_t sz, kv_spoolr_t *sp, kvset_t *set) {
  tpl_node *tn;
  char *key;
  char *val;

  kv_set_clear(set);

  tn = tpl_map("A(ss)", &key, &val);
  if (tpl_load(tn, TPL_MEM, img, sz) == -1) {
    fprintf(stderr, "tpl_load failed (sz %d)\n", (int)sz);
    return;
  }
  while( tpl_unpack(tn,1) > 0 ) {
    kv_adds(set, key, val);
    free(key);
    free(val);
  }
  tpl_free(tn);
  set->img = img;
  set->len = sz;
}

/* see if one frame is available from any spool file we're locked on */
static int read_frame(kv_spoolr_t *sp, kvset_t *set) {
  kv_spoolrec_t *r, *tmp;
  struct stat sb;
  size_t sz;
  void *img;
  int rc;

  HASH_ITER(sppath_hh, sp->spools, r, tmp) {
    if (fstat(r->sp_fd, &sb) == -1) { sp->need_rescan=1; continue; }
    if (sb.st_size < sizeof(uint32_t)) continue;
    if (sb.st_size - sizeof(uint32_t) <= r->pos) continue;
    if (lseek(r->sp_fd, r->pos, SEEK_SET) == -1) {sp->need_rescan=1; continue; }
    if (tpl_gather(TPL_GATHER_BLOCKING, r->sp_fd, &img, &sz) <= 0) {
      if (++r->retries > 3) {
        fprintf(stderr,"possible spool corruption on %s: unlinking\n", r->sppath);
        unlink(r->srpath);
        unlink(r->sppath);
        sp->need_rescan=1;
      }
      continue;
    }
    else if (r->retries) r->retries=0;
    if (update_rpos(sp,r,sz) == -1) {free(img); continue;}
    r->last = time(NULL);
    fill_set(img,sz,sp,set);
    //do not free(img); that is done in kv_set_free
    return 1; /* success */
  }
  return 0; /* no frame to read */
}

/* used to fully qualify a filename into path for use in hash lookup */
static void fully_qualify(kv_spoolr_t *sp, char *name, char **path, int *plen) {
  utstring_clear(sp->tmp); 
  utstring_printf(sp->tmp, "%s/%s", sp->dir, name);
  *path = utstring_body(sp->tmp); 
  *plen = utstring_len(sp->tmp);
}

/* block on event notification waiting for file modification */
static int wait_for_event(kv_spoolr_t *sp) {
  int rc=-1, ifd=-1;

  assert(sp && sp->dir);

  if ( (ifd = inotify_init()) == -1) goto done;
  if (inotify_add_watch(ifd, sp->dir, IN_ALL_EVENTS) == -1) {
    fprintf(stderr,"failed to inotify on %s: %s\n", sp->dir, strerror(errno));
    goto done;
  }

  fd_set inotify_fdset; FD_ZERO(&inotify_fdset); FD_SET(ifd,&inotify_fdset);
  struct timeval timeout = {.tv_sec = INTERNAL_RESCAN_INTERVAL_SEC, .tv_usec=0};
  if (select(ifd+1, &inotify_fdset, NULL, NULL, &timeout) == -1) {
    fprintf(stderr,"select error: %s\n",strerror(errno)); 
    //if (errno != EINTR) goto done;
    goto done;
  }
  rc=0;

 done:
  if (ifd != -1) close(ifd);
  sp->need_rescan=1;
  return rc;
}

/*******************************************************************************
 * Spool reader API
 ******************************************************************************/
void *kv_spoolreader_new(const char *dir) {
  assert(dir);
  int rc = -1;

  kv_spoolr_t *sp;
  if ( (sp = malloc(sizeof(*sp))) == NULL) sp_oom();
  memset(sp,0,sizeof(*sp));
  sp->dir = strdup(dir);
  sp->spools = NULL;
  sp->ev_sz = sizeof(*(sp->ev)) + PATH_MAX;
  if ( (sp->ev = malloc(sp->ev_sz)) == NULL) sp_oom();
  sp->need_rescan = 0;
  sp_readlimits(dir);
  utstring_new(sp->tmp);

  if (rescan_spools(sp) == -1) goto done;
  rc = 0; // success

 done:
  if (rc == -1) {
    if (sp->dir) free(sp->dir);
    if (sp->ev) free(sp->ev);
    if (sp->tmp) utstring_free(sp->tmp);
    assert(sp->spools == NULL);
    free(sp);
    sp = NULL;
  }
  return sp;
}

/* Here's the all important spool reader function that reads
 * a spool frame and populates the set from it. It can be 
 * blocking, which will wait for a frame to appear in the spool,
 * or non-blocking which will return if the spool doesn't have
 * any unread frames at the moment.
 */
int kv_spool_read(void *_sp, void *_set, int blocking) {
  kv_spoolr_t *sp = (kv_spoolr_t*)_sp;
  kvset_t *set = (kvset_t*)_set;
  kv_t *kv, *tmp;

  /* check whether we're due for periodic spool rescan.
   * we do this because, we don't get inotify file events
   * after handing a set back to the caller. so new spoolfiles
   * created outside of the lifetime of this function need
   * to be detected by a periodic rescan. Flag also gets set
   * if we get a change event on a spool we aren't locked on,
   * or a deletion event on a spool we are locked on.
   */
  if ((time(NULL)-sp->last_scan) > KVSPOOL_RESCAN_INTERVAL) {
    sp->need_rescan=1;
  }

 again:
  if (sp->need_rescan) {if (rescan_spools(sp) == -1) return -1;}
  if (read_frame(sp,set)) return 1;
  if (!blocking) return 0;          /* no frame ready */
  if (wait_for_event(sp) >= 0) goto again;
  return -1; /* if we got here, a blocking wait failed; error out */

}

void kv_spoolreader_free(void *_sp) {
  kv_spoolr_t *sp = (kv_spoolr_t*)_sp;
  kv_spoolrec_t *rec, *tmp;

  if (sp->dir) free(sp->dir);
  if (sp->ev) free(sp->ev);
  if (sp->tmp) utstring_free(sp->tmp);

  HASH_ITER(sppath_hh, sp->spools, rec, tmp) {
    HASH_DELETE(sppath_hh,sp->spools,rec);
    if (rec->srpath) free(rec->srpath);
    if (rec->sppath) free(rec->sppath);
    if (rec->sp_fd != -1) close(rec->sp_fd);
    if (rec->sr_fd != -1) close(rec->sr_fd);
    free(rec);
  }

  free(sp);
}

void sp_rewind(const char *dir) {
  char *path, **p;
  int sr_fd,rp;

  UT_array *files;
  utarray_new(files, &ut_str_icd);

  if (sp_readdir(dir, ".sr", files) == -1) goto done;

  p = NULL;
  while ( (p = (char**)utarray_next(files,p))) {
    path = *p;
    if ( (sr_fd = open(path, O_RDWR)) == -1) continue;
    if (flock(sr_fd, LOCK_EX|LOCK_NB) == -1) {close(sr_fd); continue;}
    lseek(sr_fd,0,SEEK_SET);
    rp = strlen(KVSPOOL_MAGIC);
    if (write(sr_fd, &rp, sizeof(rp)) != sizeof(rp)) unlink(path);
    close(sr_fd);
  }

 done:
  utarray_free(files);
}

/* get the percentage consumed for dir 
   returns -1 on error */
int kv_stat(const char *dir, kv_stat_t *stats) {
  char **f, *file;
  uint32_t sz, spsz;
  uint64_t gsz=0, gspsz=0;
  int fd, rr, rc = -1;
  struct stat sb;
  kv_stat_t *s;

  UT_array *files; utarray_new(files, &ut_str_icd);
  if (sp_readdir(dir, ".sr", files) == -1) goto done;
  memset(stats, 0, sizeof(*stats));

  f = NULL;
  while ( (f=(char**)utarray_next(files,f))) {
    file = *f;

    /* get size consumed (sz) and total spool size (spsz) for current sp/sr */
    if ( (fd=open(file,O_RDONLY)) == -1) continue;
    rr = read(fd,&sz,sizeof(sz)); close(fd); if (rr != sizeof(sz)) continue;
    file[strlen(file)-1]='p';
    spsz = 0;
    if (stat(file,&sb) != -1)  {
      spsz = sb.st_size;
      if (sb.st_mtime > stats->last_write) stats->last_write = sb.st_mtime;
    }
    if (spsz>=8) {spsz-=8; sz-=8;} else continue; /* ignore spool preamble */
    gsz += sz; gspsz += spsz;
  }
  stats->spool_sz = gspsz;
  stats->pct_consumed = gspsz ? (int)(gsz * 100.0 / gspsz) : 100;
  rc = 0;  /* success */

 done:
  utarray_free(files);
  return rc;
}
