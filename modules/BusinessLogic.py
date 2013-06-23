#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import csv
import pprint
import sys

from modules import GoogSuggestMe, StampThatBit, TweetCatcher


__author__ = 'Giannis Dzegoutanis'


class BusinessLogic:
  def __init__(self, db_directory, namespace=False):
    self._db_directory = db_directory
    self._namespace = namespace

  def __enter__(self):
    if (self._namespace):
      self._logic = Logic(self._db_directory, namespace=self._namespace)
    else:
      self._logic = Logic(self._db_directory)
    return self._logic

  def __exit__(self, type, value, traceback):
    pass


class Logic:
  def __init__(self, db_directory, namespace=False):
    self._db_directory = db_directory
    self._db_name = u'%s.db' % (unicode(namespace) if namespace else u'default')

  def _u(self, queries):
    return map(lambda x: unicode(x), queries)

  def _path(self):
    return os.path.join(self._db_directory, self._db_name)

  def list(self):
    for entry in sorted(os.listdir(self._db_directory)):
      if entry.endswith('.db'):
        print(entry.replace('.db',''))

  def write(self, queries):
    # Create a datastore and fetch data for the keyword
    with GoogSuggestMe.GoogSuggestMe(self._path()) as bucket:
      bucket.suggest(self._u(queries))
    with StampThatBit.StampThatBit(self._path()) as bucket:
      bucket.update()
    with TweetCatcher.TweetCatcher(self._path()) as bucket:
      bucket.search(queries[0])
      # bucket.start_stream(queries[0])

  def printBucket(self, bucket, time, output_format):
    print output_format
    if output_format.lower() == 'json':
      print(json.dumps(bucket.readall(time=time)))
    elif output_format.lower() == 'tsv':
      tsv = TsvWriter(sys.stdout, dialect=csv.excel_tab)
      tsv.writerows(bucket.readall(time=time))
    else:
      pprint.PrettyPrinter().pprint(bucket.readall(time=time))

  def read(self, module="TweetCatcher", time=1, output_format=None):
    if module.lower() == "GoogSuggestMe".lower():
      with GoogSuggestMe.GoogSuggestMe(self._path()) as bucket:
        self.printBucket(bucket, time, output_format)
    elif module.lower() == "StampThatBit".lower():
      with StampThatBit.StampThatBit(self._path()) as bucket:
        self.printBucket(bucket, time, output_format)
    elif module.lower() == "TweetCatcher".lower():
      with TweetCatcher.TweetCatcher(self._path()) as bucket:
        self.printBucket(bucket, time, output_format)


class TsvWriter:
  """
  A CSV writer which will write rows to CSV file "f",
  which is encoded in the given encoding.
  """

  def __init__(self, f, dialect=csv.excel, **kwds):
    self.writer = csv.writer(f, dialect=dialect, **kwds)

  def writerow(self, row):
    self.writer.writerow([s.encode("utf-8") if isinstance(s, (basestring, str)) else s for s in row])

  def writerows(self, rows):
    for row in rows:
      self.writerow(row)
