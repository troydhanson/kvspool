#ifndef _LOGC_H_
#define _LOGC_H_

void lm(char *fmt, ...);
#ifdef DEBUG
#define lmd lm
#endif //DEBUG

#endif //_LOGC_H_
