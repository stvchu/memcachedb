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

"""Test suit for MemcacheDB

"""

import unittest
import memcache

class MemcacheDBTestCase(unittest.TestCase):
  def setUp(self):
    self.mc = memcache.Client(['127.0.0.1:21201'], debug=0)

  def tearDown(self):
    self.mc.disconnect_all()
	
  def testSetCmd(self):
    self.assert_(self.mc.set("testkey_set", "testvalue_set"))
		
  def testGetCmd(self):
    self.assert_(self.mc.set("testkey_get", "testvalue_get"))
    self.assertEqual(self.mc.get("testkey_get"), "testvalue_get")
	
  def testMultiGetCmd(self):
    self.assert_(self.mc.set("testkey1_mget", "testvalue1_mget"))
    self.assert_(self.mc.set("testkey2_mget", "testvalue2_mget"))
    self.assertEqual(self.mc.get_multi(["testkey1_mget", "testkey2_mget"]), 
                     {"testkey1_mget": "testvalue1_mget", "testkey2_mget": "testvalue2_mget"})
		
  def testAddCmd(self):
    self.mc.delete("testkey_add")
    self.assert_(self.mc.add("testkey_add", "testvalue_add"))
    self.assert_(not self.mc.add("testkey_add", "testvalue_add"))
		
  def testReplaceCmd(self):
    self.mc.delete("testkey_replace")
    self.assert_(not self.mc.replace("testkey_replace", "testvalue_replace"))
    self.assert_(self.mc.set("testkey_replace", "testvalue_replace"))
    self.assert_(self.mc.replace("testkey_replace", "testvalue_replace"))
    		
  def testAppendCmd(self):
    self.mc.delete("testkey_append")
    self.assert_(not self.mc.append("testkey_append", "testvalue_append"))
    self.assert_(self.mc.set("testkey_append", "testvalue_append"))
    self.assert_(self.mc.append("testkey_append", "testvalue_append"))
		
  def testPrependCmd(self):
    self.mc.delete("testkey_prepend")
    self.assert_(not self.mc.append("testkey_prepend", "testvalue_prepend"))
    self.assert_(self.mc.set("testkey_prepend", "testvalue_prepend"))
    self.assert_(self.mc.append("testkey_prepend", "testvalue_prepend"))
    	
  def testDeleteCmd(self):
    #delete in memcache.py dose not have a good return value
    # self.assert_(not self.mc.delete("testkey_delete"))
    # self.assert_(self.mc.set("testkey_delete", "testvalue_delete"))
    # self.assert_(self.mc.delete("testkey_delete"))
    pass
    	
  def testDbArchiveCmd(self):
		self.assert_(self.mc.db_archive())
		
  def testDbCheckpointCmd(self):
		self.assert_(self.mc.db_checkpoint())
		
  def testRepSetPriorityCmd(self):
		self.assert_(self.mc.rep_set_priority(200))
		
  def testRepSetAckPolicy(self):
		self.assert_(self.mc.rep_set_ack_policy(5))
		
if __name__ == '__main__':
  suite = unittest.TestLoader().loadTestsFromTestCase(MemcacheDBTestCase)
  unittest.TextTestRunner(verbosity=2).run(suite)