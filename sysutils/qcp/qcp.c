#include <stdio.h>
#include <libgen.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/fcntl.h>
 
int verbose=0;
void usage(char *prog) {
  fprintf(stderr, "usage: %s <file> -s <ip> -p <port>  # send one file\n", prog);
  fprintf(stderr, "  ls | %s -s <ip> -p <port>         # file names on stdin\n", prog);
  exit(-1);
}

char *server = "127.0.0.1";
uint16_t port = 2000;
int md;
 
char *map(char *file, size_t *len) {
  struct stat s;
  char *buf;

  if ( (md = open(file, O_RDONLY)) == -1) {
      fprintf(stderr,"can't open %s: %s\n", file, strerror(errno));
      exit(-1);
  }
  if (fstat(md, &s) == -1) {
      close(md);
      fprintf(stderr,"can't stat %s: %s\n", file, strerror(errno));
      exit(-1);
  }
  *len = s.st_size;
  buf = mmap(0, s.st_size, PROT_READ, MAP_PRIVATE, md, 0);
  if (buf == MAP_FAILED) {
    close(md);
    fprintf(stderr, "failed to mmap %s: %s\n", file, strerror(errno));
    exit(-1);
  }
  /* don't: close(md); */
  return buf;
}

int send_file(char *filename) {
  char *buf, *base;
  size_t buflen;
  int rc,baselen;
  time_t before,after;

  buf = map(filename, &buflen);
  if (!buf) return -1;

  /**********************************************************
   * create an IPv4/TCP socket, not yet bound to any address
   *********************************************************/
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    printf("socket: %s\n", strerror(errno));
    exit(-1);
  }

  /**********************************************************
   * internet socket address structure, for the remote side
   *********************************************************/
  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = inet_addr(server);
  sin.sin_port = htons(port);

  if (sin.sin_addr.s_addr == INADDR_NONE) {
    printf("invalid remote IP %s\n", server);
    exit(-1);
  }

  /**********************************************************
   * Perform the 3 way handshake, (c)syn, (s)ack/syn, c(ack)
   *********************************************************/
  if (connect(fd, (struct sockaddr*)&sin, sizeof(sin)) == -1) {
    printf("connect: %s\n", strerror(errno));
    exit(-1);
  }

  time(&before);

  /* write a length-prefixed filename.*/
  base = basename(filename);
  baselen = strlen(base);
  if (verbose) fprintf(stderr,"sending %s\n", base);
  if (write(fd,&baselen,sizeof(baselen)) != sizeof(baselen)) {
    printf("write: %s\n", (rc<0)?strerror(errno):"incomplete");
    exit(-1);
  }

  if (write(fd,base,baselen) != baselen) {
    printf("write: %s\n", (rc<0)?strerror(errno):"incomplete");
    exit(-1);
  }

  /* and write the file content */
  if ( (rc=write(fd,buf,buflen)) != buflen) {
    printf("write: %s\n", (rc<0)?strerror(errno):"incomplete");
    exit(-1);
  }
  close(fd);
  time(&after);
  int sec = after-before;
  if (verbose) fprintf(stderr,"sent in %u sec (%.0f Mbps)\n", sec, 
    sec ? ((buflen/(1024.0*1024))/sec) :0);


  munmap(buf,buflen);
  close(md);

  if (verbose) printf("sent %s\n",filename);
}

int main(int argc, char * argv[]) {
  int opt;
  char *file=NULL, *buf;
  size_t len;
  char filename[100];
 
  while ( (opt = getopt(argc, argv, "v+s:p:h")) != -1) {
    switch (opt) {
      case 's': server = strdup(optarg); break;
      case 'p': port = atoi(optarg); break;
      case 'v': verbose++; break;
      default: usage(argv[0]); break;
    }
  }
 
  if (optind < argc) file=argv[optind++];

  if (!file) {
    while (fgets(filename,sizeof(filename),stdin) != NULL) {
      int len = strlen(filename);
      if (filename[len-1] == '\n') filename[len-1]='\0'; 
      send_file(filename);
    }
  }
  else send_file(file);

}
