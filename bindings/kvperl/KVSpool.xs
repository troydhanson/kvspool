#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#include <kvspool.h>

#include "const-c.inc"


int hash_to_kvs(HV *hash, void *set) {
    SV *pv;
    int rc = -1;
    char *k, *v;
    int len = 0;

    while ((pv = hv_iternextsv(hash,&k,(I32*)&len))!= 0) {
      STRLEN le;
      v = SvPV(pv,le);
      if (!k || !v) goto done;
      kv_add(set, k,len, v,le);
    }
    rc = 0;
   done:
    return rc;
}

SV** kvs_to_hash(HV **hash, void *set) {
    SV *pv; //,*pk;
    SV **rc = NULL;

    *hash = newHV();

    kv_t *kv=NULL;
    while ( (kv= kv_next(set,kv))) {

      pv = newSVpv(kv->val,kv->vlen);
      rc = hv_store(*hash,kv->key,kv->klen,pv,0);

      if (rc == NULL) break;
    }

    if (rc == NULL) {
      hv_undef(*hash);
      *hash = NULL;
    }
    return rc;
}
/*
*/



MODULE = KVSpool		PACKAGE = KVSpool		

INCLUDE: const-xs.inc


SV*
makeset()
PREINIT:
void *set;
CODE:

    set = kv_set_new();
    if ( set == NULL) {
      croak("cannot initialize set");
    }
    SV*  pv = newSViv(PTR2IV(set));
    RETVAL = pv;
OUTPUT:
    RETVAL



SV*
makersp(char * dir)
PREINIT:
void *sp;
CODE:

    if ( (sp = kv_spoolreader_new(dir)) == NULL) {
      croak("cannot initialize spool reader");
    }
    //IV pv = PTR2IV(sp);
    SV*  pv = newSViv(PTR2IV(sp));
//    printf("%i %i\n",SvUV(pv),sp);
 //printf("IV is %"IVdf" %i\n", pv,sp);
    
    RETVAL = pv;//newRV(pv);
OUTPUT:
    RETVAL




SV*
makewsp(char * dir)
PREINIT:
void *sp;
CODE:

    if ( (sp = kv_spoolwriter_new(dir)) == NULL) {
      croak("cannot initialize spool reader");
    }
    SV*  pv = newSViv(PTR2IV(sp));
    
    RETVAL = pv;//newRV(pv);
OUTPUT:
    RETVAL

void 
freeset(IV pos)
INIT:
    void *set;
CODE:
    set = INT2PTR(void*,pos);
    kv_set_free(set);



void 
freersp(IV pos)
INIT:
    void *sp;
CODE:
    sp = INT2PTR(void*,pos);
    kv_spoolreader_free(sp);


void
freewsp(IV pos)
INIT:
    void *sp;
CODE:
    sp = INT2PTR(void*,pos);
    kv_spoolwriter_free(sp);



HV*
kvread(IV pos,void *set, int block)
INIT:
    void *sp;
    HV *hash = NULL;
    int rc =0;
    SV** val;
CODE:
    sp = INT2PTR(void*,pos);
    
    if ( (rc=kv_spool_read(sp,set,block)) > 0) {
      kvs_to_hash(&hash, set);
      RETVAL = hash;
    } else if (rc == 0) {  /* non blocking read, no data available */
      XSRETURN_UNDEF;
    } else if (rc < 0) {
      croak("internal error in spool reader");
    }
OUTPUT:
    RETVAL
    

void
kvwrite(IV pos, void *set, HV* hash)
INIT:
    void *sp;
CODE:
    sp = INT2PTR(void*,pos);
    if (hash_to_kvs(hash, set) == -1) {
      croak("non-string key or value");
    }
    kv_spool_write(sp,set);



HV*
kv_stat(char * dir)
INIT:
    int sc, i;
    SV** rc = NULL;
    kv_stat_t stats;
    HV *hash = NULL;
    SV *pv;
CODE:

    sc = kv_stat(dir,&stats);
    if (sc == -1) {
      croak("kv_stat failed");
    }

    hash = newHV();
    pv = sv_2mortal(newSViv((long)(stats.pct_consumed)));
    rc = hv_store(hash,"pct",strlen("pct"),pv,0);
    if (rc == NULL) {
      sv_2mortal((SV*)hash);
      hash = NULL;
    }

    RETVAL = hash;
OUTPUT:
    RETVAL



