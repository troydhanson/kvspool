#include <arpa/inet.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include "utarray.h"
#include "utstring.h"
#include "kvsp-bconfig.h"

UT_array /* of string */ *output_keys;
UT_array /* of string */ *output_defaults;
UT_array /* of int */    *output_types;

#define x(t) #t,
char *supported_types_str[] = { TYPES };
#undef x

int parse_config(char *config_file) {
  char line[100];
  FILE *file;
  int rc=-1;
  int type,t;
  char *sp,*nl,*def;
  if ( (file = fopen(config_file,"r")) == NULL) {
    fprintf(stderr,"can't open %s: %s\n", config_file, strerror(errno));
    goto done;
  }
  while (fgets(line,sizeof(line),file) != NULL) {
    sp = strchr(line,' ');
    if (!sp) {
      fprintf(stderr,"syntax error in %s\n", config_file);
      goto done;
    }
    nl = strchr(line,'\n'); if (nl) *nl='\0';
    for(t=0; t<adim(supported_types_str); t++) {
      if(!strncmp(supported_types_str[t],line,sp-line)) break;
    }
    if (t >= adim(supported_types_str)){
      fprintf(stderr,"unknown type %s\n",line); 
      goto done;
    }
    char *id = sp+1;
    sp = strchr(id,' ');
    if (sp) *sp = '\0';
    def = sp ? sp+1 : NULL;
    utarray_push_back(output_types,&t);
    utarray_push_back(output_keys,&id);
    utarray_push_back(output_defaults,&def);
  }
  rc = 0;
 done:
  if (file) fclose(file);
  return rc;
}

int set_to_binary(void *set, UT_string *bin) {
  uint32_t l, u, a,b,c,d,e,f, abcd;
  struct in_addr ia4;
  struct in6_addr ia6;
  uint16_t s;
  uint8_t g;
  double h;
  utstring_clear(bin);
  l=0; utstring_bincpy(bin,&l,sizeof(l)); // placeholder for size prefix
  int rc=-1,i=0,*t;
  kv_t *kv, kvdef;
  char **k=NULL,**def;
  while( (k=(char**)utarray_next(output_keys,k))) {
    kv = kv_get(set,*k);
    t = (int*)utarray_eltptr(output_types,i); assert(t);
    def = (char**)utarray_eltptr(output_defaults,i); assert(def);
    if (kv==NULL) { /* no such key */
      kv=&kvdef;
      if (*def) {kv->val=*def; kv->vlen=strlen(*def);} /* default */
      else if (*t == str) {kv->val=NULL; kv->vlen=0;}  /* zero len string */
      else {
        fprintf(stderr,"required key %s not present in spool frame\n", *k);
        goto done;
      }
    }
    switch(*t) {
      case d64: h=atof(kv->val); utstring_bincpy(bin,&h,sizeof(h)); break;
      case i8:  g=atoi(kv->val); utstring_bincpy(bin,&g,sizeof(g)); break;
      case i16: s=atoi(kv->val); utstring_bincpy(bin,&s,sizeof(s)); break;
      case i32: u=atoi(kv->val); utstring_bincpy(bin,&u,sizeof(u)); break;
      case str8: 
        g=kv->vlen; utstring_bincpy(bin,&g,sizeof(g)); /* length prefix */
        utstring_bincpy(bin,kv->val,g);                /* string itself */
        break;
      case str: 
        l=kv->vlen; utstring_bincpy(bin,&l,sizeof(l)); /* length prefix */
        utstring_bincpy(bin,kv->val,kv->vlen);         /* string itself */
        break;
      case mac: 
        if ((sscanf(kv->val,"%x:%x:%x:%x:%x:%x",&a,&b,&c,&d,&e,&f) != 6) ||
           (a > 255 || b > 255 || c > 255 || d > 255 || e > 255 || f > 255)) {
          fprintf(stderr,"invalid MAC for key %s: %s\n",*k,kv->val);
          goto done;
        }
        g=a; utstring_bincpy(bin,&g,sizeof(g));
        g=b; utstring_bincpy(bin,&g,sizeof(g));
        g=c; utstring_bincpy(bin,&g,sizeof(g));
        g=d; utstring_bincpy(bin,&g,sizeof(g));
        g=e; utstring_bincpy(bin,&g,sizeof(g));
        g=f; utstring_bincpy(bin,&g,sizeof(g));
        break;
      case ipv46: 
        memset(&ia4, 0, sizeof(ia4));
        memset(&ia6, 0, sizeof(ia6));
        if (inet_pton(AF_INET, kv->val, &ia4) == 1) {
          assert( 4 == sizeof(struct in_addr));
          g=4; utstring_bincpy(bin,&g,sizeof(g));
          utstring_bincpy(bin, &ia4, sizeof(struct in_addr));
        } else if (inet_pton(AF_INET6, kv->val, &ia6) == 1) {
          assert( 16 == sizeof(struct in6_addr));
          g=16; utstring_bincpy(bin,&g,sizeof(g));
          utstring_bincpy(bin, &ia6, sizeof(struct in6_addr));
        } else {
          fprintf(stderr,"invalid IP for key %s: %s\n",*k,kv->val);
          goto done;
        }
        break;
      case ipv4: 
        if ((sscanf(kv->val,"%u.%u.%u.%u",&a,&b,&c,&d) != 4) ||
           (a > 255 || b > 255 || c > 255 || d > 255)) {
          fprintf(stderr,"invalid IP for key %s: %s\n",*k,kv->val);
          goto done;
        }
        abcd = (a << 24) | (b << 16) | (c << 8) | d;
        abcd = htonl(abcd);
        utstring_bincpy(bin,&abcd,sizeof(abcd));
        break;
      default: assert(0); break;
    }
    i++;
  }
  uint32_t len = utstring_len(bin); len -= sizeof(len); // length does not include itself
  char *length_prefix = utstring_body(bin);
  memcpy(length_prefix, &len, sizeof(len));

  rc = 0;

 done:
  return rc;
}

static int get(void **msg_data,size_t *msg_len,void *dst,size_t len) {
  if (*msg_len < len) {
    fprintf(stderr,"received message shorter than expected\n"); 
    return -1;
  }
  memcpy(dst,*msg_data,len);
  *(char**)msg_data += len;
  *msg_len -= len;
  return 0;
}

int binary_to_frame(void *sp, void *set, void *msg_data, size_t msg_len, UT_string *tmp) {
  int rc=-1,i=0,*t;
  const char *key;
  struct in_addr ia;
  char dst[INET6_ADDRSTRLEN];
  char src[16];

  uint32_t l, u, a,b,c,d, abcd;
  uint16_t s;
  uint8_t g;
  double h;
  uint8_t m[6];

  kv_set_clear(set);
  char **k = NULL;
  while ( (k=(char**)utarray_next(output_keys,k))) {
    t = (int*)utarray_eltptr(output_types,i); assert(t);
    // type is *t and key is *k
    utstring_clear(tmp);
    switch(*t) {
      case d64: if (get(&msg_data,&msg_len,&h,sizeof(h))<0) goto done; utstring_printf(tmp,"%f",h); break;
      case i8:  if (get(&msg_data,&msg_len,&g,sizeof(g))<0) goto done; utstring_printf(tmp,"%d",(int)g); break;
      case i16: if (get(&msg_data,&msg_len,&s,sizeof(s))<0) goto done; utstring_printf(tmp,"%d",(int)s); break;
      case i32: if (get(&msg_data,&msg_len,&u,sizeof(u))<0) goto done; utstring_printf(tmp,"%d",u); break;
      case str8:
        if (get(&msg_data,&msg_len,&g,sizeof(g)) < 0) goto done;
        utstring_reserve(tmp,g);
        if (get(&msg_data,&msg_len,utstring_body(tmp),g) < 0) goto done;
        tmp->i += g;
        break;
      case str:
        if (get(&msg_data,&msg_len,&l,sizeof(l)) < 0) goto done;
        utstring_reserve(tmp,l);
        if (get(&msg_data,&msg_len,utstring_body(tmp),l) < 0) goto done;
        tmp->i += l;
        break;
      case ipv4:
        if (get(&msg_data,&msg_len,&abcd,sizeof(abcd)) < 0) goto done;
        ia.s_addr = abcd;
        utstring_printf(tmp,"%s", inet_ntoa(ia));
        break;
      case ipv46:
        if (get(&msg_data,&msg_len,&g,sizeof(g)) < 0) goto done;
        assert((g == 4) || (g == 16));
        if (get(&msg_data,&msg_len,src,g) < 0) goto done;
        if (inet_ntop((g == 4) ? AF_INET : AF_INET6, src, dst, sizeof(dst)) == NULL) {
          fprintf(stderr, "inet_ntop: %s\n", strerror(errno));
          goto done;
        }
        utstring_printf(tmp,"%s", dst);
        break;
      case mac:
        if (get(&msg_data,&msg_len,m,sizeof(m)) < 0) goto done;
        utstring_printf(tmp,"%2.2x:%2.2x:%2.2x:%2.2x:%2.2x:%2.2x",
           m[0], m[1], m[2], m[3], m[4], m[5]);
        break;
      default: assert(0); break;
    }
    i++;
    key = *k;
    kv_add(set, key, strlen(key), utstring_body(tmp), utstring_len(tmp));
  }
  kv_spool_write(sp, set);

  rc = 0;

 done:
  if (rc) fprintf(stderr,"binary frame mismatches expected message length\n");
  return rc;
}
