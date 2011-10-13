#include "logc.h"
char *modname = "test2";
int main(int argc, char *argv[]) {
  char *who = (fork()) ? "parent" : "child";
  int iter=1;
  if (argc > 1) iter = atoi(argv[1]);
  while(iter--) lm("hello %d from %s", iter, who);
}
