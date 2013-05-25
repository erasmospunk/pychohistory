#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlite3
import os

__author__ = 'Giannis Dzegoutanis'

DB_VERSION = 1


class Bucket:
  def __init__(self, bucket_path):
    self._path = bucket_path

  def __enter__(self):
    """
    Create an instance of the IoBucket class
    """
    self._io_bucket = IoBucket(self._path)
    return self._io_bucket

  def __exit__(self, type, value, traceback):
    self._io_bucket.close()


class IoBucket:
  """ Bucket that contains the data of a period """

  def __init__(self, db_path):
    """
    :type db_path: str
    :param db_path:
    """

    # If not exist, create path
    if not os.path.exists(os.path.dirname(db_path)):
      os.makedirs(os.path.dirname(db_path))

    self._con = sqlite3.connect(db_path)
    self._check_db()

  def _check_db(self):
    cur = self._con.cursor()

    # Check if PychoHistory table exists
    cur.execute(""" SELECT name FROM sqlite_master WHERE type='table' AND name='PychoHistory' """)

    if cur.fetchone() is None:
      cur.execute(""" CREATE TABLE PychoHistory(Version INTEGER, Description TEXT) """)
      cur.execute(""" INSERT INTO PychoHistory VALUES(?, ?) """, (DB_VERSION, None))
      cur.execute(""" CREATE TABLE Data
                    ( _id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME, key TEXT, value REAL ) """)
      self._con.commit()

    cur.execute(""" SELECT Version FROM PychoHistory """)
    self._version = cur.fetchone()[0]
    if self._version > DB_VERSION:
      self._con.close()
      raise Exception("The database version is newer than the version this client supports: v%d > v%d" % (
        self._version, DB_VERSION))

  def db_connection(self):
    return self._con

  def write(self, dbInput):
    cur = self._con.cursor()
    cur.executemany(""" INSERT INTO Data VALUES(NULL, ?, ?, ?) """, dbInput)
    self._con.commit()

  def read(self, query=None):
    cur = self._con.cursor()
    cur.execute(""" SELECT * FROM Data """)
    results = cur.fetchall()
    return results

  def version(self):
    return self._version

  def update_version(self, new_version):
    cur = self._con.cursor()
    cur.execute(""" UPDATE PychoHistory SET Version=? """, (new_version, ))
    self._con.commit()
    self._version = new_version

  def close(self):
    self._con.close()
