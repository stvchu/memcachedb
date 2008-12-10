#!/bin/sh
./memcachedb -p21201 -d -r -m 256 -L 1024 -H ./testenv -N -v >log 2>&1 
#./memcachedb -p21201 -d -r -L 256 -H ./testenv0 -N -v -R 127.0.0.1:31201 -M -n 2 >log_m 2>&1
#sleep 5
#./memcachedb -p21202 -d -r -L 256 -H ./testenv1 -N -v -R 127.0.0.1:31202 -O 127.0.0.1:31201 -S -n 2 > log_s 2>&1
