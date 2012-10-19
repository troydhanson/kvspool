#include <stdio.h>
#include <string.h>
#include "utarray.h"
#include "utstring.h"
#include <errno.h>
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
    sp = strchr(line,' '); if (!sp) continue;
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
