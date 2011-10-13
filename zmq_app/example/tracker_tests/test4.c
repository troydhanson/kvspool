#include <stdio.h>
#include <time.h>
#include "tracker.h"

time_t when = 1317213882;

int main() {
  int i;
  tracker_t *t = tracker_new(10,5);
  for(i=0;i<10;i++) tracker_hit(t,"1",when); 
  for(i=0;i<9;i++) tracker_hit(t,"2",when); 
  for(i=0;i<8;i++) tracker_hit(t,"3",when); 
  for(i=0;i<7;i++) tracker_hit(t,"4",when); 
  for(i=0;i<6;i++) tracker_hit(t,"5",when); 
  for(i=0;i<5;i++) tracker_hit(t,"6",when); 
  for(i=0;i<4;i++) tracker_hit(t,"7",when); 
  for(i=0;i<3;i++) tracker_hit(t,"8",when); 
  for(i=0;i<2;i++) tracker_hit(t,"9",when); 
  for(i=0;i<1;i++) tracker_hit(t,"10",when); 

  show_tracker(t);
  show_tracker_top(t);

  for(i=0;i<10;i++) tracker_hit(t,"11",when); 
  show_tracker(t);
  show_tracker_top(t);

  tracker_hit(t,"12",when); 
  show_tracker(t);
  show_tracker_top(t);

  tracker_free(t);
  return 0;
}
