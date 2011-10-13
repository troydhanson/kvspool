#!/bin/bash

SPW=../../tests/spw 
export PYTHONPATH="../build/lib.linux-i686-2.6"

# sanity checks
if [ ! -d spool ]; then mkdir spool; fi
if [ ! -x ${SPW} ]; then echo "script requires ${SPW}"; exit -1; fi

# write out a frame and read it from python
${SPW} -b base spool
./read.py
