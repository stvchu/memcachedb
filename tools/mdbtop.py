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

"""a unix top style stat tool for MemcacheDB

"""

import sys
import time
import signal
import curses
import memcache
import getopt

class TopDisplayer(object):
  def __init__(self):
    # initialize the curse
    self.stdscr = curses.initscr()
    curses.noecho()
    curses.cbreak()
    
  def close(self):
    # finalize the curse
    curses.nocbreak()
    self.stdscr.keypad(0)
    curses.echo()
    curses.endwin()
    
  def display(self, y, x, text):
    self.stdscr.addstr(y, x, text)
    self.stdscr.refresh()


def main(server, interval):    
  # connect to mdb
  mc = memcache.Client([server], debug=0)
  
  # display header
  top.display(0, 0, "Press Ctrl+C to quit.")
  top.display(1, 0, "%-20s  %10s  %10s  %10s  %10s  %10s  %10s" % ("Server:", "Uptime", "Conns", "read/sec", "wrtn/sec", "sets/sec", "gets/sec"))
  
  read_per_sec = 0
  wrtn_per_sec = 0
  gets_per_sec = 0
  sets_per_sec = 0
  
  # get the initial value
  stats = mc.get_stats()
  if stats == []:
    return
  stats_dict = stats[0][1]
  conns = int(stats_dict["curr_connections"])
  uptime = int(stats_dict["uptime"])
  bytes_read = int(stats_dict["bytes_read"])
  bytes_written = int(stats_dict["bytes_written"])
  cmd_gets = int(stats_dict["cmd_get"])
  cmd_sets = int(stats_dict["cmd_set"])

  while True:
    stats = mc.get_stats()
    if stats == []:
      continue
    stats_dict = stats[0][1]
    conns = int(stats_dict["curr_connections"])
    uptime = int(stats_dict["uptime"])
    read_per_sec = (int(stats_dict["bytes_read"]) - bytes_read)/interval
    bytes_read = int(stats_dict["bytes_read"])
    wrtn_per_sec = (int(stats_dict["bytes_written"]) - bytes_written)/interval
    bytes_written = int(stats_dict["bytes_written"])
    gets_per_sec = (int(stats_dict["cmd_get"]) - cmd_gets)/interval
    cmd_gets = int(stats_dict["cmd_get"])
    sets_per_sec = (int(stats_dict["cmd_set"]) - cmd_sets)/interval
    cmd_sets = int(stats_dict["cmd_set"])
    top.display(2, 0, "%-20s  %10d  %10d  %10d  %10d  %10d  %10d" % (server, uptime, conns, read_per_sec, wrtn_per_sec, sets_per_sec, gets_per_sec))
    time.sleep(interval)
  
  # close the connection
  mc.disconnect_all()

def usage():
  print 'Usage: python mdbben.py [options]'
  print 'Options are:'
  print '  --server=<ip:port>, -s <ip:port>     Server that connects to, default is \'127.0.0.1:21201\''
  print '  --interval=<interval>, -i <interval> Number of threads to run the benchmark, default is 2'
  print '  --help, -h                           Display usage information (this message)'
  print

def handler(signum, frame):
  top.close()
  sys.exit(0)

if __name__ == "__main__":
  signal.signal(signal.SIGTERM, handler)
  signal.signal(signal.SIGQUIT, handler)
  signal.signal(signal.SIGINT, handler)
  
  top_cfg = {'server': '127.0.0.1:21201',
             'interval': 2}
  try:
    opts, args = getopt.getopt(sys.argv[1:], "s:i:h", 
                 ["server=", "interval=", "help"])
  except getopt.GetoptError, err:
    print str(err) 
    usage()
    sys.exit(2)
  
  for o, a in opts:
    if o in ("-s", "--server"):
      top_cfg["server"] = a
    elif o in ("-i", "--interval"):
      top_cfg["interval"] = int(a)
    elif o in ("-h", "--help"):
      usage()
      sys.exit()
    else:
      assert False, "unhandled option"
  
  top = TopDisplayer()
  main(top_cfg["server"], top_cfg["interval"])
  top.close()
  