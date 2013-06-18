#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import datetime
import logging
import unittest
import os
import shutil

from modules import Datastore, GoogSuggestMe, BusinessLogic


__author__ = 'Giannis Dzegoutanis'


TEMP_FOLDER = u'tmp'
TEST_DATABASE = u'testdatastore.tmp'
TEST_QUERY = u'hello world'

datastore_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), TEMP_FOLDER)

class TestBusinessLogic(unittest.TestCase):
  def setUp(self):
    self.log = logging.getLogger()

  def tearDown(self):
    try:
      shutil.rmtree(datastore_path, True)
    except:
      pass

  def test_write_read(self):
    with BusinessLogic.BusinessLogic(datastore_path) as logic:
      logic.write(TEST_QUERY)
      logic.read()

  def test_write_read_with_db_name(self):
    with BusinessLogic.BusinessLogic(datastore_path, namespace=TEST_DATABASE) as logic:
      logic.write(TEST_QUERY)
      logic.read()

