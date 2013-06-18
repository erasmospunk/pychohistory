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
TEST_QUERY = u'test'

module_name = u'test_module'
module_signature = (module_name,
                    (u'timestamp', u'key', u'value'),
                    (datetime.utcnow(), u'keyword', 1244000))

TEST_SRC = u'test_src_name'
TEST_SRC_PARAMS = dict(param1=u'test text', param2=13.37)

datastore_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), TEMP_FOLDER)
test_db_path = os.path.join(datastore_path, TEST_DATABASE)


class TestDatabase(unittest.TestCase):
  def setUp(self):
    self.log = logging.getLogger()

  def tearDown(self):
    try:
      shutil.rmtree(datastore_path, True)
    except:
      pass

  def test_bucket_open(self):
    """ Test if the database opens """
    with Datastore.Bucket(test_db_path, module_signature) as bucket:
      self.assertNotEqual(bucket, None, u'Bucket is None')

  def test_update_version(self):
    with Datastore.Bucket(test_db_path, module_signature) as bucket:
      new_ver = bucket.version() + 1
      bucket.update_version(new_ver)
      self.assertEqual(bucket.version(), new_ver, u'Failed updating to v%d' % new_ver)

    with self.assertRaises(Exception):
      Datastore.IoBucket(test_db_path)

  def test_create_invalid_src(self):
    """ Create one invalid data source """
    with self.assertRaises(Exception):
      bad_sig = (TEST_SRC, (u'param1', u'param2'), (u'text', [u'array is unsupported']))
      with Datastore.Bucket(test_db_path, bad_sig):
        pass

    with self.assertRaises(Exception):
      bad_sig = (TEST_SRC, (u'param1', u'param2'), (u'text', ))
      with Datastore.Bucket(test_db_path, bad_sig):
        pass

  def test_bucket_read_write(self):
    """ Test if can write to database"""

    now = datetime.utcnow()
    test_key_vals = [
      (now, "ideas to write about", 645000000),
      (now, "ideas to go", 2260000000),
      (now, "ideas to raise money", 106000000),
      (now, "ideas to ask someone to prom", 966000),
      (now, "ideas to ask a guy to prom", 378000),
      (now, "ideas to build in minecraft", 7710000),
      (now, "ideas unlimited", 217000000),
      (now, "ideas united", 1530000000),
      (now, "ideas ucla", 10700000),
      (now, "ideas unlimited pepsi", 7190000),
      (now, "ideas unlimited seminars", 1800000),
      (now, "ideas unbound", 4310000),
      (now, "ideas unlimited llc", 68000000),
      (now, "ideas unlimited memphis", 1650000),
      (now, "ideas uthscsa", 133000),
      (now, "ideas ucsb", 609000),
      (now, "ideas vs ideals", 7920000),
      (now, "ideas valentines day", 123000000),
      (now, "ideas valentines coupons", 2480000)
    ]
    with Datastore.Bucket(test_db_path, module_signature) as bucket:
      bucket.insertmany(test_key_vals)

    with Datastore.Bucket(test_db_path, module_signature) as bucket:
      self.assertEqual(len(bucket.readall()), len(test_key_vals))

  def test_bucket_read_write_single(self):
    """ Test if can write to database"""

    now = datetime.utcnow()
    with Datastore.Bucket(test_db_path, module_signature) as bucket:
      bucket.insert((now, "single", "2200"))
      bucket.insert((now, "test", "1200"))

    with Datastore.Bucket(test_db_path, module_signature) as bucket:
      self.assertEqual(len(bucket.readall()), 2)

  def test_bucket_duplicates_read_write(self):
    """ Test if can write to database"""

    now = datetime.utcnow()
    test_key_vals = [
      (now, "ideas to build in minecraft", 7710000),
      (now, "ideas unlimited", 217000000),
      (now, "ideas to build in minecraft", 7710000),
      (now, "ideas unlimited", 217000000),
      (now, "ideas to build in minecraft", 7710000),
      (now, "ideas unlimited", 217000000)
    ]

    with Datastore.Bucket(test_db_path, module_signature) as bucket:
      bucket.insertmany(test_key_vals)

    with Datastore.Bucket(test_db_path, module_signature) as bucket:
      self.assertEqual(len(bucket.readall()), len(test_key_vals))




