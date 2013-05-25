#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import pprint

from modules import Datastore, GoogSuggestMe


__author__ = 'Giannis Dzegoutanis'


class BusinessLogic:
  def __init__(self, db_directory):
    self._db_directory = db_directory

  def __enter__(self):
    self._logic = Logic(self._db_directory)
    return self._logic

  def __exit__(self, type, value, traceback):
    pass


class Logic:
  def __init__(self, db_directory):
    self._db_directory = db_directory

  def _u(self, queries):
    return map(lambda x: unicode(x), queries)

  def write(self, queries):
    for query in self._u(queries):
      # Create a datastore and fetch data for the keyword
      with Datastore.Bucket(os.path.join(self._db_directory, query)) as bucket:
        with GoogSuggestMe.GoogSuggestMe(bucket) as goog:
          goog.suggest(query)

  def read(self, queries, output_format=None):
    for query in self._u(queries):
      # Print the datastore to the stdio TODO return valid json and python
      pp = pprint.PrettyPrinter()
      with Datastore.Bucket(os.path.join(self._db_directory, query)) as bucket:
        if output_format is 'json':
          print(json.dumps(bucket.read()))
        else:
          pp.pprint(bucket.read())



