#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import datetime
import logging
import unittest
import os
import shutil

from modules import db


__author__ = 'Giannis Dzegoutanis'

TEMP_FOLDER = "tmp"
TEST_DATABASE = "testdatastore.tmp"

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
    with db.Bucket(test_db_path) as bucket:
      self.assertNotEqual(bucket, None, "Bucket is None")

  def test_update_version(self):
    with db.Bucket(test_db_path) as bucket:
      new_ver = bucket.version() + 1
      bucket.update_version(new_ver)
      self.assertEqual(bucket.version(), new_ver, "Failed updating to v%d" % new_ver)

    with self.assertRaises(Exception):
      db.IoBucket(test_db_path)

  def test_bucket_read_write(self):
    """ Test if can write to database"""

    now = datetime.utcnow()
    test_key_vals = [
      (now, "bitcoin", "17300000"),
      (now, "bitcoinnews", "129000"),
      (now, "bitcoin mining", "12100000"),
      (now, "bitcoinnordic", "4300"),
      (now, "bitcoin exchange", "39300000"),
      (now, "bitcoin price", "91000000"),
      (now, "bitcoin nyc", "11400000"),
      (now, "bitcoin charts", "25100000"),
      (now, "bitcoin to usd", "5620000"),
      (now, "bitcoin calculator", "3040000"),
      (now, "bitcoin otc", "64700"),
      (now, "bitcoinity", "192000")
    ]
    with db.Bucket(test_db_path) as bucket:
      bucket.write(test_key_vals)

    with db.Bucket(test_db_path) as bucket:
      self.assertEqual(len(bucket.read()), len(test_key_vals))

  def test_bucket_duplicates_read_write(self):
    """ Test if can write to database"""

    now = datetime.utcnow()
    test_key_vals = [
      (now, "bitcoin", "17300000"),
      (now, "bitcoin", "17300000"),
      (now, "bitcoin mining", "12100000")
    ]

    with db.Bucket(test_db_path) as bucket:
      bucket.write(test_key_vals)

    with db.Bucket(test_db_path) as bucket:
      self.assertEqual(len(bucket.read()), len(test_key_vals))




