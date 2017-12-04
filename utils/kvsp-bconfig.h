#include <stdio.h>
#include <string.h>
#include "utarray.h"
#include "utstring.h"
#include "kvspool_internal.h"

extern UT_array /* of string */ *output_keys;
extern UT_array /* of string */ *output_defaults;
extern UT_array /* of int */    *output_types;

int parse_config(char *);
int set_to_binary(void *set, UT_string *bin);
int binary_to_frame(void *sp, void *set, void *msg_data, size_t msg_len, UT_string *tmp);


extern char *supported_types_str[];

#define TYPES x(i16) x(i32) x(ipv4) x(ipv46) x(str) x(str8) x(i8) x(d64) x(mac)
#define x(t) t,
enum supported_types { TYPES };
#undef x
#define adim(a) (sizeof(a)/sizeof(*a))
