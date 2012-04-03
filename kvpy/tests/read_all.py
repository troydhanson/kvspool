#!/usr/bin/python

# so we don't have to install kvpy.so (or we could 
# have set PYTHONPATH to include its dir beforehand)
import sys
sys.path.append("../build/lib.linux-i686-2.6")

import kvspool
kv = kvspool.Kvspool("spool")
kv.blocking = 0
while True:
  d = kv.read()
  if (d == None):
    break
  for key in d.keys():
    print "key: " + key + " value: " + d[key]
