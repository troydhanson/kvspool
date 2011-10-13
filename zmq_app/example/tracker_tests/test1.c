#include <stdio.h>
#include "tracker.h"

int main() {
  tracker_t *t = tracker_new(10,5);
  tracker_free(t);
  return 0;
}
