#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import pprint
from datetime import datetime

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
    now = datetime.utcnow()
    self._db_name = u'%d-%02d-%02d.db' % (now.year, now.month, now.day)
    if namespace:
      self._db_name = u'%s.%s' % (unicode(namespace), self._db_name)

  def _u(self, queries):
    return map(lambda x: unicode(x), queries)

  def _path(self):
    return os.path.join(self._db_directory, self._db_name)

  def write(self, queries):
    # Create a datastore and fetch data for the keyword
    with GoogSuggestMe.GoogSuggestMe(self._path()) as bucket:
      bucket.suggest(self._u(queries))
    with StampThatBit.StampThatBit(self._path()) as bucket:
      bucket.update()
    with TweetCatcher.TweetCatcher(self._path()) as bucket:
      bucket.search(queries[0])
      # bucket.start_stream(queries[0])

  def printBucket(self, bucket, output_format):
    pp = pprint.PrettyPrinter()
    if output_format is 'json':
      print(json.dumps(bucket.readall()))
    else:
      pp.pprint(bucket.readall())

  def read(self, output_format=None):
    # Print the datastore to the stdio TODO return valid json and python
    # with GoogSuggestMe.GoogSuggestMe(self._path()) as bucket:
    #   self.printBucket(bucket, output_format)
    #
    # with StampThatBit.StampThatBit(self._path()) as bucket:
    #   self.printBucket(bucket, output_format)
    #
    # with TweetCatcher.TweetCatcher(self._path()) as bucket:
    #   self.printBucket(bucket, output_format)

    with TweetCatcher.TweetCatcher(self._path()) as bucket:
      self.printBucket(bucket, output_format)
