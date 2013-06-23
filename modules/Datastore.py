#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import sqlite3
import os
from datetime import date

__author__ = 'Giannis Dzegoutanis'

log = logging.getLogger(u'TweetCatcher')

DB_VERSION = 2

TYPE_TEXT = u'TEXT'
TYPE_REAL = u'REAL'
TYPE_INTEGER = u'INTEGER'
TYPE_DATETIME = u'DATETIME'
TYPES = [TYPE_TEXT, TYPE_REAL, TYPE_INTEGER, TYPE_DATETIME]


def value_to_type(value):
  if isinstance(value, (basestring, str)):
    return TYPE_TEXT
  elif isinstance(value, (int, long)):
    return  TYPE_INTEGER
  elif isinstance(value, float):
    return  TYPE_REAL
  elif isinstance(value, date):
    return TYPE_DATETIME
  else:
    raise Exception("Unrecognized type '%s' (%s)" % (value, type(value)))


class Bucket:
  def __init__(self, bucket_path, module_signature=None):
    self._path = bucket_path
    self._module_signature = module_signature

  def __enter__(self):
    """
    Create an instance of the IoBucket class
    """
    if (self._module_signature):
      self._io_bucket = ModuleIoBucket(self._path, self._module_signature)
    else:
      self._io_bucket = IoBucket(self._path)
    return self._io_bucket

  def __exit__(self, type, value, traceback):
    self._io_bucket.close()



class IoBucket(object):
  """ Bucket that contains the data """

  def __init__(self, db_path):
    """
    @type db_path: str
    """

    # If database folder doesn't exist, create the path
    if not os.path.exists(os.path.dirname(db_path)):
      os.makedirs(os.path.dirname(db_path))

    self._con = sqlite3.connect(db_path)
    self._check_db()

  def _check_db(self):
    cur = self._con.cursor()

    # Check if PychoHistory table exists
    cur.execute(u" SELECT name FROM sqlite_master WHERE type='table' AND name='PychoHistory' ")

    if cur.fetchone() is None:
      cur.execute(u" CREATE TABLE PychoHistory(Version INTEGER, Description TEXT) ")
      cur.execute(u" INSERT INTO PychoHistory VALUES(?, ?) ", (DB_VERSION, None))
      self._con.commit()

    cur.execute(u" SELECT Version FROM PychoHistory ")
    self._version = cur.fetchone()[0]
    if self._version != DB_VERSION:
      self._con.close()
      raise Exception("The database version is different than the version this client supports: v%d > v%d" % (
        self._version, DB_VERSION))

    # Check if PychoHistory table exists
    cur.execute(u" SELECT name FROM sqlite_master WHERE type='table' AND name='PychoHistoryModules' ")

    if cur.fetchone() is None:
      cur.execute(u" CREATE TABLE PychoHistoryModules(Name TEXT, Signature TEXT) ")

  def io(self, module_signature):
    return ModuleIoBucket(self.path, module_signature)

  def db_connection(self):
    return self._con

  def version(self):
    return self._version

  def update_version(self, new_version):
    cur = self._con.cursor()
    cur.execute(u" UPDATE PychoHistory SET Version=? ", (new_version, ))
    self._con.commit()
    self._version = new_version

  def close(self):
    self._con.close()


class ModuleIoBucket(IoBucket):
  """ Bucket that contains the data of a period """

  def _name(self):
    (n, f, t) = self._module_signature
    return n

  def _features(self):
    (n, f, t) = self._module_signature
    return f

  def _types(self):
    (n, f, t) = self._module_signature
    return t

  def __init__(self, db_path, module_signature):
    """
    @type db_path: str
    @module_signature: (name, features, feature_types)
    """

    super(ModuleIoBucket, self).__init__(db_path)

    (name, features, example_values) = module_signature

    if len(features) != len(example_values):
      raise Exception("Invalid signature, features and values must match in size. "
                      "%s != %s" % (len(features), len(example_values)))

    types = tuple(map(value_to_type, example_values))
    features = tuple(features)

    self._set_module_signature(name, features, types)
    self._register_module()
    self._create_queries()

  def _set_module_signature(self, name, features, types):
    self._module_signature = (name, features, types)

  def _create_queries(self):
    self._query_insert = self._create_insert_query()
    self._query_select_all = self._create_select_all_query()

  def _create_table_query(self):
    (name, features, types) = self._module_signature

    types_query = ", ".join([" ".join(v) for v in zip(features, types)])
    return u" CREATE TABLE %s ( _id INTEGER PRIMARY KEY AUTOINCREMENT, %s ) " % (name, types_query)

  def _create_module_query(self):
    return u" INSERT INTO PychoHistoryModules VALUES(?, ?) "

  def _create_select_module(self):
    return u" SELECT * FROM PychoHistoryModules WHERE Name='%s' " % self._name()

  def _create_insert_query(self):
    num_of_features = len(self._features())
    features_query = ", ".join(["?" for _ in xrange(num_of_features)])
    return u" INSERT INTO %s VALUES(NULL, %s ) " % (self._name(), features_query)

  def _create_select_all_query(self):
    return u" SELECT * FROM %s ORDER BY timestamp DESC" % self._name()

  def _check_signature(self, signature_raw):
    (name, features, types) = tuple(json.loads(signature_raw))
    sig = (name, tuple(features), tuple(types))

    if sig != self._module_signature:
      raise Exception("Signatures don't match %s and %s" % (str(sig), str(self._module_signature)))

  def _register_module(self):
    module_entry = self._fetchone(self._create_select_module())

    if module_entry:
      (name, signature_raw) = module_entry
      self._check_signature(signature_raw)
    else:
      self._execute(self._create_table_query())
      self._execute(self._create_module_query(), (self._name(), json.dumps(self._module_signature)))

  def _fetchone(self, query, params=None):
    cur = self._execute(query, params)
    return cur.fetchone()

  def _fetchall(self, query, params=None):
    cur = self._execute(query, params)
    return cur.fetchall()

  def _execute(self, query, params=None):
    log.debug(u'Going to execute query "%s" with params "%s"' % (unicode(query), unicode(params)))
    cur = self._con.cursor()
    if params:
      cur.execute(query, params)
    else:
      cur.execute(query)
    self._con.commit()
    return cur

  def _executemany(self, query, data):
    cur = self._con.cursor()
    cur.executemany(query, data)
    self._con.commit()

  def insert(self, dbInput):
    self._execute(self._query_insert, dbInput)

  def insertmany(self, dbInput):
    self._executemany(self._query_insert, dbInput)

  def readall(self, time=1):
    return self._fetchall(self._query_select_all)



