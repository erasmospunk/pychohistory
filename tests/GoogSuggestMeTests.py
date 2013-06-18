#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import datetime
import logging
import unittest
import os
import shutil

from modules import Datastore, GoogSuggestMe


__author__ = 'Giannis Dzegoutanis'

TEMP_FOLDER = u'tmp'
TEST_DATABASE = u'testdatastore.tmp'
TEST_QUERY1 = u'hello world'
TEST_QUERY2 = u'bye world'

datastore_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), TEMP_FOLDER)
test_db_path = os.path.join(datastore_path, TEST_DATABASE)


class TestGoogSuggestMe(unittest.TestCase):
  def setUp(self):
    self.log = logging.getLogger()

  def tearDown(self):
    try:
      shutil.rmtree(datastore_path, True)
    except:
      pass

  def test_suggest(self):
    with GoogSuggestMe.GoogSuggestMe(test_db_path) as goog:
      goog.suggest(TEST_QUERY1)
      self.assertNotEqual(len(goog.readall()), 0)

  def test_suggest_multiple_dict(self):
    with GoogSuggestMe.GoogSuggestMe(test_db_path) as goog:
      goog.suggest([TEST_QUERY1, TEST_QUERY2])
      self.assertNotEqual(len(goog.readall()), 0)

  def test_suggest_multiple_params(self):
    with GoogSuggestMe.GoogSuggestMe(test_db_path) as goog:
      goog.suggest(TEST_QUERY1, TEST_QUERY2)
      self.assertNotEqual(len(goog.readall()), 0)