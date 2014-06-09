#include <stdio.h>
#include <time.h>
#include <netinet/in.h>
#include <net/if.h>
#include <netdb.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "kvspool.h"
#include "kvspool_internal.h"
#include "utarray.h"
#include "utstring.h"

int verbose;
enum {console,network} mode = console;
/* these are used in network mode to specify the udp destination */
in_addr_t addr; 
int port; 
char *iface; // used for multicast with explicitly specified nic
int interval=10; /* udp report interval (seconds) */

void usage(char *prog) {
  fprintf(stderr, "display mode:  %s [-v] spool ...\n", prog);
  fprintf(stderr, "network mode:  %s -u udp://239.0.0.1:9999[@eth2] [-t 10] spool ...\n", prog);
  exit(-1);
}

static int parse_spec(char *spec) {
  char *proto = spec, *colon, *host, *at;
  struct hostent *h;
  int hlen, rc=-1;

  if (strncmp(proto, "udp://", 6)) goto done;
  host = &spec[6];

  if ( !(colon = strrchr(spec, ':'))) goto done;
  port = atoi(colon+1);
  if ((port < 0) || (port > 65535)) goto done;

  if ( (at = strrchr(spec, '@')) != NULL) { // trailing @eth2
    iface = at+1;
  }

  /* dns lookup. */
  *colon = '\0'; 
  h = gethostbyname(host); 
  hlen = strlen(host);
  *colon = ':';
  if (!h) {
    fprintf(stderr, "lookup [%.*s]: %s", hlen, host, hstrerror(h_errno));
    rc = -2;
    goto done;
  }

  addr = ((struct in_addr*)h->h_addr)->s_addr;
  rc = 0; /* success */

 done:
  if (rc == -1) fprintf(stderr,"required format: udp://1.2.3.4:5678[@eth2]");
  return rc;
}

void log_status(char*dir) {
  int fd=-1,sc,i;
  char **f, *file;
  time_t now, elapsed;
  UT_array *files;
  utarray_new(files,&ut_str_icd);
  struct stat sb;
  uint32_t sz, spsz;

  printf("%-40s ", dir);

  kv_stat_t stats;
  sc = kv_stat(dir,&stats);
  if (sc == -1) {
    fprintf(stderr,"kv_stat error in %s\n", dir);
    goto done; 
  }
  UT_string *s;
  utstring_new(s);
  utstring_printf(s,"%3u%% ", stats.pct_consumed);
  long lz = stats.spool_sz;
  char *unit = "";
  if      (lz > 1024*1024*1024){unit="gb "; lz/=(1024*1024*1024); }
  else if (lz > 1024*1024)     {unit="mb "; lz/=(1024*1024);      }
  else if (lz > 1024)          {unit="kb "; lz/=(1024);           }
  else                         {unit="b ";                        }
  utstring_printf(s,"%10lu%s", (long)lz, unit);

  unit="";
  now = time(NULL);
  elapsed =  now - stats.last_write;
  if      (elapsed > 60*60*24) {unit="days";  elapsed/=(60*60*24);}
  else if (elapsed > 60*60)    {unit="hours"; elapsed/=(60*60);   }
  else if (elapsed > 60)       {unit="mins";  elapsed/=(60);      }
  else if (elapsed >= 0)       {unit="secs";                      }
  if (stats.last_write == 0)   utstring_printf(s,"%10snever","");
  else utstring_printf(s,"%10lu%s", (long)elapsed, unit);
  printf("%s\n", utstring_body(s));
  utstring_free(s);

  /* the rest is for verbose output */
  if (!verbose) goto done;
  printf("\n\n");
  if (sp_readdir(dir, ".sr", files) == -1) goto done;
  f = NULL;
  while ( (f=(char**)utarray_next(files,f))) {
    file = *f;
    printf("\t%s ",file);
    if ( (fd=open(file,O_RDONLY)) == -1) {
      perror("cannot open");
      goto done;
    }
    if (read(fd,&sz,sizeof(sz)) != sizeof(sz)) {
      perror("cannot open");
      close(fd);
      goto done;
    }
    close(fd);
    file[strlen(file)-1]='p';
    if (stat(file,&sb) == -1) spsz = 0;
    else spsz = sb.st_size;
    /* ignore the spool preamble */
    if (spsz) spsz-=8;
    sz -= 8;
    printf("%u/%u (%2.2f%%)\n", sz, spsz, (spsz?(sz*100.0/spsz):0));
  }

 done:
  utarray_free(files);
}

void udp_status(UT_array *dirs) {
  int rc;
  kv_stat_t stats;
  char **dir;

  UT_string *s;
  utstring_new(s);
  utstring_printf(s,"kvsp-status\n");

  dir=NULL;
  while ( (dir=(char**)utarray_next(dirs,dir))) {
    rc = kv_stat(*dir,&stats);
    if (rc == -1) {
      fprintf(stderr,"kv_stat error in %s\n", *dir);
      goto done; 
    }
    long lz = stats.spool_sz;
    char *unit = "";
    if      (lz > 1024*1024*1024){unit="gb"; lz/=(1024*1024*1024); }
    else if (lz > 1024*1024)     {unit="mb"; lz/=(1024*1024);      }
    else if (lz > 1024)          {unit="kb"; lz/=(1024);           }
    else                         {unit="b";                        }
    utstring_printf(s,"%s ", *dir);
    utstring_printf(s,"%u%% ", stats.pct_consumed);
    utstring_printf(s,"%lu%s ", (long)lz, unit);
    unit="";
    time_t now = time(NULL);
    time_t elapsed =  now - stats.last_write;
    if      (elapsed > 60*60*24) {unit="days";  elapsed/=(60*60*24);}
    else if (elapsed > 60*60)    {unit="hours"; elapsed/=(60*60);   }
    else if (elapsed > 60)       {unit="mins";  elapsed/=(60);      }
    else if (elapsed >= 0)       {unit="secs";                      }
    if (stats.last_write == 0)   utstring_printf(s,"never\n");
    else utstring_printf(s,"%lu%s\n", (long)elapsed, unit);
  }
  char *buf = utstring_body(s);
  int len = utstring_len(s);


  /**********************************************************
   * create an IPv4/UDP socket, not yet bound to any address
   *********************************************************/
  int fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd == -1) {
    fprintf(stderr,"socket: %s\n", strerror(errno));
    goto done;
  }

  /* use a specific NIC if one was specified, supported here for multicast */
  if (iface) {
    int l = strlen(iface);
    if (l+1 >IFNAMSIZ) {fprintf(stderr,"interface too long\n"); goto done;}

    struct ifreq ifr;
    ifr.ifr_addr.sa_family = AF_INET;
    memcpy(ifr.ifr_name, iface, l+1);

    /* does this interface support multicast? */
    if (ioctl(fd, SIOCGIFFLAGS, &ifr)) {fprintf(stderr,"ioctl: %s\n", strerror(errno)); goto done;} 
    if (!(ifr.ifr_flags & IFF_MULTICAST)) {fprintf(stderr,"%s does not multicast\n",iface); goto done;}

    /* get the interface IP address */
    struct in_addr iface_addr;
    if (ioctl(fd, SIOCGIFADDR, &ifr)) {fprintf(stderr,"ioctl: %s\n", strerror(errno)); goto done;} 
    iface_addr = (((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr);

    /* ask kernel to use its IP address for outgoing multicast */
    if (setsockopt(fd, IPPROTO_IP, IP_MULTICAST_IF, &iface_addr, sizeof(iface_addr))) {
      fprintf(stderr,"setsockopt: %s\n", strerror(errno));
      goto done;
    }
  }

  /**********************************************************
   * internet socket address structure, for the remote side
   *********************************************************/
  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = addr;
  sin.sin_port = htons(port);

  if (sin.sin_addr.s_addr == INADDR_NONE) {
    fprintf(stderr,"invalid remote IP\n");
    goto done;
  }

  /**********************************************************
   * UDP is connectionless; connect only sets dest for writes
   *********************************************************/
  if (connect(fd, (struct sockaddr*)&sin, sizeof(sin)) == -1) {
    fprintf(stderr,"connect: %s\n", strerror(errno));
    goto done;
  }

  if (verbose) {
    fprintf(stderr,"udp packet:\n%.*s\n", len, buf);
  }

  if ( (rc=write(fd,buf,len)) != len) {
    fprintf(stderr,"write: %s\n", (rc<0)?strerror(errno):"incomplete");
    goto done;
  }

 done:
  utstring_free(s);
  return;
}

 
int main(int argc, char * argv[]) {
  char **dir;
  int i, opt;
  UT_array *dirs;
  utarray_new(dirs,&ut_str_icd);

  while ( (opt = getopt(argc, argv, "v+u:t:h")) != -1) {
    switch (opt) {
      case 'v': verbose++; break;
      case 'u': mode=network; parse_spec(optarg); break;
      case 't': interval = atoi(optarg); break;
      case 'h': default: usage(argv[0]); break;
    }
  }
  if (optind >= argc) usage(argv[0]);
  while(optind < argc) utarray_push_back(dirs, &argv[optind++]);

  switch(mode) {
    case console:
      dir=NULL;
      while ( ( dir=(char**)utarray_next(dirs,dir))) log_status(*dir);
      break;
    case network:
      while(1) {
        udp_status(dirs);
        sleep(interval);
      }
      break;
  }

 done:
  utarray_free(dirs);
  return 0;
}

