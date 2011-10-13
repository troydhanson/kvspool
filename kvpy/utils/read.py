#!/usr/bin/python

# so we don't have to install kvpy.so (or we could 
# have set PYTHONPATH to include its dir beforehand)
import sys
sys.path.append("../build/lib.linux-i686-2.6")

import kvpy
d = kvpy.kvpy_read("spool","base",0)
for key in d.keys():
  print "key: " + key + " value: " + d[key]
