#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2008 Steve Chu.  All rights reserved.
# 
# Use and distribution licensed under the BSD license.  See
# the LICENSE file for full text.
# 
# Authors:
#     Steve Chu <stvchu@gmail.com>

"""Benchmark suit for MemcacheD/MemcacheDB

This is a multiple-threads benchmark suit for daemon over memcache protocol
"""

import memcache
import random
import time
import sys
import threading
import thread
import getopt

ben_result = []
ben_result_mutex = thread.allocate_lock( )

class MemcacheBenchCase(object):
  def __init__(self, ben_cfg):
    self.threads_ = []
    self.ben_cfg_ = ben_cfg
    
  def run(self):
      print 'Benchmarking (be patient).....'
      for i in range(self.ben_cfg_['threads']):
        if self.ben_cfg_['command'] == 'SET':
          thread = Setter(self.ben_cfg_['server'], self.ben_cfg_['requests'], self.ben_cfg_['length'])
        elif self.ben_cfg_['command'] == 'GET':
          thread = Getter(self.ben_cfg_['server'], self.ben_cfg_['requests'], self.ben_cfg_['length'])
        else:
          print 'unknown command'
          sys.exit(1)
        thread.start()
        self.threads_.append(thread)
      
  def join(self):
    for thread in self.threads_:
      thread.join()
    self.print_result()
    
  def print_result(self):
    total_time_cost = 0.0
    total_errors = 0
    total_threads = self.ben_cfg_['threads']
    requests_per_thread = self.ben_cfg_['requests']
    total_requests = total_threads * requests_per_thread
    value_length = self.ben_cfg_['length']
    
    # stats the total
    for (thread_name, time_cost_per_thread, errors_per_thread) in ben_result:
      total_time_cost = total_time_cost + time_cost_per_thread
      total_errors = total_errors + errors_per_thread

    # calculate all fields  
    avg_time_cost = total_time_cost / total_threads
    throughout = total_requests * (value_length + 16) / avg_time_cost / 1024
    requests_per_second = total_requests / avg_time_cost
    time_cost_per_request = avg_time_cost / total_requests * 1000
    
    # print out
    print 'done.'
    print
    print 'Server name: %s' % self.ben_cfg_['server']
    print 'Command: %s' % self.ben_cfg_['command']
    print 'Thread number: %d' % total_threads
    print 'Requests per thread: %d' % requests_per_thread
    print 'Value Length: %d bytes' % value_length
    print 'Avg. time cost per thread: %f seconds' % avg_time_cost
    print 'Throughout: %d kbytes/sec' % throughout
    print 'Requests per second: %d req/sec' % requests_per_second
    print 'Time cost per request: %f ms' % time_cost_per_request
    print 'Total requests: %d' % total_requests
    print 'Total errors: %d' % total_errors

class Setter(threading.Thread):
  def __init__(self, server, requests, value_length):
    self.mc_ = memcache.Client([server], debug=1)
    self.requests_ = requests
    self.value_length_ = value_length
    threading.Thread.__init__(self)
    
  def run(self):
    begin = time.time()
    errors = 0
    for i in range(self.requests_):
      ret = self.mc_.set("test-%011d" % (i,), "*" * self.value_length_)
      if not ret:
        errors = errors + 1
    end = time.time()
    # print 'ok, time cost: %d' % (end - begin)
    ben_result_mutex.acquire()
    ben_result.append((self.getName(), end - begin, errors))
    ben_result_mutex.release()
    print 'Thread name: %s; Time cost: %f seconds; Requests: %d; Errors: %d' % (self.getName(),
          end - begin, self.requests_, errors)
  
  def __del__(self):
    self.mc_.disconnect_all()
    
class Getter(threading.Thread):
  def __init__(self, server, requests, value_length):
    self.mc_ = memcache.Client([server], debug=0)
    self.requests_ = requests
    self.value_length_ = value_length
    threading.Thread.__init__(self)

  def run(self):
    begin = time.time()
    errors = 0
    for i in range(self.requests_):
      ret = self.mc_.get("test-%011d" % (i,))
      if ret == None or len(ret) != self.value_length_:
        errors = errors + 1;
    end = time.time()
    ben_result_mutex.acquire()
    ben_result.append((self.getName(), end - begin, errors))
    ben_result_mutex.release()
    print 'Thread name: %s; Time cost: %f seconds; Requests: %d; Errors: %d' % (self.getName(),
          end - begin, self.requests_, errors)

  def __del__(self):
    self.mc_.disconnect_all()

def usage():
  print 'Usage: python mdbben.py [options]'
  print 'Options are:'
  print '  --server=<ip:port>, -s <ip:port>     Server that the suit connects to, default is \'127.0.0.1:21201\''
  print '  --command=<command>, -c <command>    Operation that intends to run, \'SET\' or \'GET\', default is \'SET\''
  print '  --threads=<threads>, -t <threads>    Number of threads to run the benchmark, default is 4'
  print '  --requests=<requests>, -n <requests> Number of requests to perform, default is 100000 per thread'
  print '  --length=<length>, -l <length>'
  print '                                       Length of an object value, default is 100 btyes'
  print '  --help, -h                           Display usage information (this message)'
  print

def main(args):
  ben_cfg = {'server': '127.0.0.1:21201',
             'command': 'SET',
             'threads': 4,
             'requests': 100000,
             'length': 100}
  try:
    opts, args = getopt.getopt(sys.argv[1:], "s:c:t:n:l:h", 
                 ["server=", "command=", "threads=", "requests=", "length=", "help"])
  except getopt.GetoptError, err:
    print str(err) 
    usage()
    sys.exit(2)
  
  for o, a in opts:
    if o in ("-s", "--server"):
      ben_cfg["server"] = a
    elif o in ("-c", "--command"):
      ben_cfg["command"] = a
    elif o in ("-t", "--threads"):
      ben_cfg["threads"] = int(a)
    elif o in ("-n", "--requests"):
      ben_cfg["requests"] = int(a)
    elif o in ("-l", "--length"):
      ben_cfg["length"] = int(a)
    elif o in ("-h", "--help"):
      usage()
      sys.exit()
    else:
      assert False, "unhandled option"
  
  bench = MemcacheBenchCase(ben_cfg)
  bench.run()
  bench.join()

if __name__ == '__main__':
  main(sys.argv)
