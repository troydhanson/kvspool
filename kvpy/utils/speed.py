#!/usr/bin/python

# so we don't have to install kvpy.so (or we could 
# have set PYTHONPATH to include its dir beforehand)
import sys
sys.path.append("../build/lib.linux-i686-2.6")

from datetime import datetime;

import kvpy
d = {"key":"value","key2":"value2"}

# write test
t1 = datetime.now()
for i in range(100000):
  kvpy.kvpy_write("/tmp/spool",d)
t2 = datetime.now()
t3 = t2 - t1
print "write:", int(100 / (t3.seconds + (t3.microseconds/1000000.0))), "kfps"

# read test
t1 = datetime.now()
for i in range(100000):
  kvpy.kvpy_read("/tmp/spool",0)
t2 = datetime.now()
t3 = t2 - t1
print "read: ", int(100 / (t3.seconds + (t3.microseconds/1000000.0))), "kfps"

