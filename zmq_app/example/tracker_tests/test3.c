#include <stdio.h>
#include <time.h>
#include "tracker.h"

time_t when = 1317213882;

int main() {
  tracker_t *t = tracker_new(10,5);
  tracker_hit(t,"1",when); 
  tracker_hit(t,"1",when); 
  tracker_hit(t,"2",when); 
  tracker_hit(t,"3",when); 
  tracker_hit(t,"4",when); 
  tracker_hit(t,"5",when); 
  tracker_hit(t,"6",when); 
  tracker_hit(t,"7",when); 
  tracker_hit(t,"8",when); 
  tracker_hit(t,"9",when); 
  tracker_hit(t,"10",when); 
  show_tracker(t);
  show_tracker_top(t);

  tracker_hit(t,"11",when);
  show_tracker(t);
  show_tracker_top(t);

  tracker_hit(t,"2",when);
  show_tracker(t);
  show_tracker_top(t);

  tracker_hit(t,"12",when); 
  show_tracker(t);
  show_tracker_top(t);

  tracker_free(t);
  return 0;
}
