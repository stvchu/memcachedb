#!/bin/bash

#./memcachedb -p21201 -d -r -H ./testenv -N -v >log 2>&1
./memcachedb -p21201 -d -r -H ./testenv0 -N -R 127.0.0.1:31201 -M -n 2 -v >log_m 2>&1
sleep 5
./memcachedb -p21202 -d -r -H ./testenv1 -N -R 127.0.0.1:31202 -O 127.0.0.1:31201 -S -n 2 -v >log_s 2>&1
