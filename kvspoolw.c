#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <stdio.h>

#include "utstring.h"
#include "utarray.h"
#include "tpl.h"

#include "kvspool.h"
#include "kvspool_internal.h"

typedef struct {
  char *dir;
  char *path;
  int fd;
  size_t noc;
} kv_spoolw_t;

#define MIN(x,y) (x<y?x:y)
kv_spool_options_t kv_spool_options = {
  .dir_max = KVSPOOL_DIR_MAX,
  .file_max = MIN(KVSPOOL_DIR_MAX/10,KVSPOOL_FILE_MAX),
};

int sp_strsort(const void *_a, const void *_b) {
  char *a = *(char**)_a;
  char *b = *(char**)_b;
  return strcmp(a,b);
}

void sp_readlimits(const char *dir) {
  long dir_max;
  char b[20];
  int fd,rc,cv;
  char *p, unit;
  UT_string *s;
  utstring_new(s);
  utstring_printf(s,"%s/limits",dir);
  p = utstring_body(s);
  if ( (fd = open(p, O_RDONLY)) != -1) {
    if ( (rc=read(fd, b, sizeof(b)-1)) > 0) {
      b[rc] = '\0';
      cv = sscanf(b,"%ld%c", &dir_max, &unit);
      switch(cv) {
        case 2: /* convert number w/units to bytes */ 
          switch(unit) {
            case 't': case 'T': dir_max *= 1024; /* fall through */
            case 'g': case 'G': dir_max *= 1024; /* fall through */
            case 'm': case 'M': dir_max *= 1024; /* fall through */
            case 'k': case 'K': dir_max *= 1024; break;
            case '\r': case '\n': case ' ': case '\t': break;
            default: /* unsupported unit */ goto done;
          }
        /* fall through to case 1 */
        case 1: kv_spool_options.dir_max = dir_max; break;
      }
    }
  }

 done:
  if (fd != -1) close(fd);
  utstring_free(s);
  kv_spool_options.file_max = MIN(kv_spool_options.dir_max/10,KVSPOOL_FILE_MAX);
}

int sp_readdir(const char *dir, const char *suffix, UT_array *files) {
  int rc = -1, blen, nlen, slen;
  struct dirent *dent;
  char *name, *path;
  UT_string *s;
  DIR *d;

  utstring_new(s);
  utarray_clear(files);
  slen = suffix ? strlen(suffix) : 0;

  if ( (d = opendir(dir)) == NULL) {
    fprintf(stderr, "failed to opendir [%s]: %s\n", dir, strerror(errno));
    goto done;
  }

  while ( (dent = readdir(d)) != NULL) {
    if (dent->d_type != DT_REG) continue;
    name = dent->d_name;
    nlen = strlen(name);
    // verify suffix match 
    if (slen && (nlen < slen)) continue;
    if (slen && memcmp(&name[nlen-slen],suffix,slen)) continue;
    // ok, fully qualify it and push it 
    utstring_clear(s);
    utstring_printf(s, "%s/%s", dir, name);
    path = utstring_body(s);
    utarray_push_back(files, &path);
  }
  utarray_sort(files, sp_strsort);
  rc = 0; // success 

 done:
  utstring_free(s);
  if (d) closedir(d);
  return rc;
}

/* sequence number is the last number in a filename eg: spool.1234567.123-004.sp
 * this function retains only names which are the highest in each sequence */
void sp_keep_maxseq(UT_array *files) {
  char *path, **p, *last=NULL, *hyphen;
  int i,llen;
  UT_string *s;
  utstring_new(s);
  UT_array *ms_files;
  utarray_new(ms_files, &ut_str_icd);

  utarray_sort(files, sp_strsort);
  p=NULL;
  while( (p=(char**)utarray_next(files,p))) {
    path = *p;
    last = utstring_body(s);
    hyphen = strchr(last,'-');
    llen = hyphen ? (hyphen-last) : 0;
    if (llen && memcmp(path,last,llen)) {
      utarray_push_back(ms_files,&last);
    }
    utstring_clear(s);
    utstring_bincpy(s,path,strlen(path));
  }
  if (last) utarray_push_back(ms_files,&last);

  utarray_clear(files);
  utarray_inserta(files,ms_files,0);

  utstring_free(s);
  utarray_free(ms_files);
}

/* first word of spool file is a magic marker */
static int has_spool_magic(int fd, int create) {
  struct stat sb;
  int len = strlen(KVSPOOL_MAGIC); assert(len == 8);
  char mark[8],sig[3];
  int rc=-1;
  off_t of;

  if (create) {
    if (write(fd,KVSPOOL_MAGIC,8) != 8) goto done;
  } else {
    lseek(fd,0,SEEK_SET);
    if (read(fd,mark,8) != 8) goto done;
    if (memcmp(mark,KVSPOOL_MAGIC,8)) goto done;
    if ( (of=lseek(fd,0,SEEK_END)) > 8) {
      if (lseek(fd,-1*sizeof(of),SEEK_END) == -1) goto done;
      if (read(fd, &of, sizeof(of)) != sizeof(of)) goto done;
      if (lseek(fd,-1*(of+sizeof(of)),SEEK_END) == -1) goto done;
      if (read(fd, sig, sizeof(sig)) != sizeof(sig)) goto done;
      if (memcmp(sig,"tpl",sizeof(sig))) goto done;
    }
  }
  rc=0; /* success */

 done:
  return rc;
}

// just change trailing .sp to .sr and verify existence
// returns 0 on success; -1 on error / srmark not found
static int has_srmark_file(char *path, int create) {
  int plen = strlen(path);
  char *rpath = NULL;
  struct stat sb;
  int fd=-1, rc = -1, rp;

  /* sanity check - path should end in .sp */
  if (plen < 3) goto done;
  if (memcmp(&path[plen-3],".sp",3)) goto done;

  /* change .sp to .sr */
  rpath = strdup(path);
  rpath[plen-1] = 'r'; 
  if (stat(rpath, &sb) == 0) { rc = 0; goto done; }  /* success */
  if (!create) goto done;

  /* srmark file creation requested */
  umask(0); /* really want others to be able to write the srmark file */
  if ( (fd=open(rpath, O_WRONLY|O_CREAT|O_EXCL, 0766)) == -1) goto done;
  if (flock(fd, LOCK_EX|LOCK_NB) == -1) goto done;
  lseek(fd,0,SEEK_SET);
  rp = strlen(KVSPOOL_MAGIC);
  if (write(fd, &rp, sizeof(rp)) != sizeof(rp)) {unlink(rpath); goto done;}
  rc = 0;

 done:
  if (fd != -1) close(fd);
  if (rpath) free(rpath);
  return rc;
}

/* attempt to open this file for writing and lock it */
static int verify_and_lock(char *path, int create) {
  int fd=-1;
  struct stat sb;
  int mode = O_RDWR|(create ? O_CREAT : 0);

  if (( (fd=open(path, mode, 0744)) == -1)  ||
      (flock(fd, LOCK_EX|LOCK_NB)   == -1)  ||
      (fstat(fd, &sb)               == -1)  ||
      (has_spool_magic(fd,create)   == -1)  ||
      (has_srmark_file(path,create) == -1)  ||
      (sb.st_size > kv_spool_options.file_max)) 
  { 
    if (fd != -1) { close(fd); fd=-1; }
    goto done;
  }

  if (sb.st_size) lseek(fd,-1*(sizeof(off_t)),SEEK_END);

 done:
  return fd;
}

static int lock_output_spool(const char *dir, kv_spoolw_t *sp) {
  char *name, *path, **p;
  int fd = -1;
  int ts,rnd;
  DIR *d;

  UT_array *files;
  utarray_new(files, &ut_str_icd);
  UT_string *s;
  utstring_new(s);

  sp_readdir(dir, ".sp", files);
  sp_readlimits(dir);
  sp_keep_maxseq(files);
  p = NULL;
  while ( (p = (char**)utarray_next(files,p))) {
    path = *p;
    fd = verify_and_lock(path,0);
    if (fd != -1) goto done; // locked an existing spool; done
  }

  /* need to make a new spool file. make a distinct name for it */
  ts = (int)time(NULL);
  rnd = (((unsigned)(getpid()) & 0xf) << 4) | (rand() & 0xf);
  utstring_clear(s);
  utstring_printf(s,"%s/kv.%u.%.3u-0.sp", dir, ts, rnd);
  path = utstring_body(s);

  if ( (fd=verify_and_lock(path,1)) == -1) {
    fprintf(stderr, "failed to open output spool [%s]\n", path);
  }

 done:
  if (fd != -1) {
    sp->fd = fd;
    sp->path = strdup(path);
    sp->dir = strdup(dir);
  }
  utarray_free(files);
  utstring_free(s);
  return fd;
}

/* open a new file with an incremented sequence number. e.g., if the 
 * original is   /tmp/myspool/spool.123456789.999-000.sp 
 * new one is    /tmp/myspool/spool.123456789.999-001.sp  
*/
static int kv_spoolwriter_reopen(kv_spoolw_t *sp) {
  assert(sp->path);
  int fd, rc = -1, seq;
  char *path, *hyph, *seqp;

  path = strdup(sp->path);
  hyph = strrchr(path,'-'); if (!hyph) goto done;
  seqp = hyph + 1;
  if (sscanf(seqp, "%u", &seq) != 1) goto done;
  seq++;
  sprintf(seqp,"%u.sp", seq);

  if ( (fd = verify_and_lock(path,1)) == -1) goto done;

  /* success. store the new path and fd */
  free(sp->path); 
  if (sp->fd != -1) close(sp->fd); 
  sp->path = path;
  sp->fd = fd;
  sp->noc = 0;
  rc = 0;

 done:
  if (rc == -1) {
    fprintf(stderr, "failed to re-open output spool [%s]\n", path);
    free(path);
  }
  return rc;
}


/*******************************************************************************
 * Spool writer API
 ******************************************************************************/
void *kv_spoolwriter_new(const char *dir) {
  assert(dir);

  kv_spoolw_t *sp;
  if ( (sp = malloc(sizeof(*sp))) == NULL) sp_oom();
  memset(sp,0,sizeof(*sp));
  sp->dir = NULL;
  sp->path = NULL;
  sp->fd = -1;
  sp->noc = 0; /* schedule attrition scan for next write */

  if (lock_output_spool(dir,sp) == -1) { free(sp); sp=NULL; }
  return sp;
}

/* check if one of the spools' files has reached its max size (reopen)
 * or if the spool directory as a whole has reached its limit (attrition) */
static void check_sizes(kv_spoolw_t *sp, int force_reopen) {
  struct stat sb;

  if (  force_reopen               || 
       (fstat(sp->fd, &sb) == -1)  || 
       (sb.st_size > kv_spool_options.file_max)) kv_spoolwriter_reopen(sp);

  if (sb.st_size > sp->noc) {
    sp_attrition(sp->dir); 
    sp->noc += (kv_spool_options.file_max / 2); 
  }
}

/* used internally when kvsp-sub gets a serialized frame off network */
int kv_write_raw_frame(void*_sp, void *img, size_t len) {
  kv_spoolw_t *sp = (kv_spoolw_t*)_sp; assert(sp);
  int rc = -1;

  if (write(sp->fd, img, len) != len) {
    fprintf(stderr, "write to %s failed: %s\n", sp->path, strerror(errno));
    goto done;
  }
  rc = 0;

 done:
  check_sizes(sp, rc);
  return rc;
}

int kv_spool_write(void*_sp, void *_set) {
  kv_spoolw_t *sp = (kv_spoolw_t*)_sp;
  kvset_t *set = (kvset_t*)_set;
  tpl_node *tn = NULL;
  char *buf=NULL; 
  char *key, fmt;
  size_t len;
  tpl_bin v;
  int rc=-1;

  /* generate frame */
  tn = tpl_map("A(scB)", &key, &fmt, &v);
  kv_t *kv = NULL;
  while ( (kv = kv_next(set, kv))) {
    key = kv->key;
    fmt = kv->fmt;
    v.sz = kv->vlen; v.addr = kv->val;
    tpl_pack(tn,1);
  }

  tpl_dump(tn, TPL_MEM, &buf, &len);
  if (buf==NULL || len==0) goto done;
  if (write(sp->fd, buf, len) != len) goto done;
  if (write(sp->fd, &len, sizeof(len)) != sizeof(len)) goto done;
  lseek(sp->fd, -1*sizeof(len), SEEK_CUR);
  rc=0;

  /* spool file and spool directory size check */
 done:
  tpl_free(tn);
  if (buf) free(buf);
  check_sizes(sp, rc);
  return rc;
}

void kv_spoolwriter_free(void*_sp) {
  assert(_sp);
  kv_spoolw_t *sp = (kv_spoolw_t*)_sp;
  if (sp->dir)  free(sp->dir);
  if (sp->path) free(sp->path);
  if (sp->fd != -1) close(sp->fd);
  free(sp);
}


