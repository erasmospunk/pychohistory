#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import unittest
import os
import shutil

from modules import StampThatBit

__author__ = 'Giannis Dzegoutanis'

TEMP_FOLDER = u'tmp'
TEST_DATABASE = u'testdatastore.tmp'
TEST_QUERY1 = u'hello world'
TEST_QUERY2 = u'bye world'

datastore_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), TEMP_FOLDER)
test_db_path = os.path.join(datastore_path, TEST_DATABASE)


class TestStampThatBit(unittest.TestCase):
  def setUp(self):
    self.log = logging.getLogger()

  def tearDown(self):
    try:
      shutil.rmtree(datastore_path, True)
    except:
      pass

  def test_update(self):
    with StampThatBit.StampThatBit(test_db_path) as stamp:
      self.assertEqual(len(stamp.readall()), 0)
      stamp.update();
      data = stamp.readall()
      self.assertNotEqual(len(data), 0)
      self.assertEqual(len(data[0]), len(StampThatBit.module_features) + 1)  # plus 1 for datastore _id
