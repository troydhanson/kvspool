#include "logc.h"
const char *modname = "test1";
int main(int argc, char *argv[]) {
  int iter=1;
  if (argc > 1) iter = atoi(argv[1]);
  while(iter--) lm("hello %d from %s", iter, argv[0]);
}
