#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
import sys
from modules.Datastore import ModuleIoBucket

import modules.bitstamp.client as bsclient

__author__ = 'Giannis Dzegoutanis'


## PychoHistory Module ########

module_name = u'StampThatBit'
module_features = (u'timestamp', u'volume', u'last', u'bid', u'high', u'low', u'ask')
example_values = (datetime.utcnow(), 8389.16385357, 120.88, 120.88, 123.66, 116.32, 121.03)

module_signature = (module_name, module_features, example_values)

###############################


class StampThatBit:
  def __init__(self, bucket_path ):
    self._path = bucket_path

  def __enter__(self):
    self._stamp = Stamp(self._path)
    return self._stamp

  def __exit__(self, type, value, traceback):
    self._stamp.close()


class Stamp(ModuleIoBucket):
  def __init__(self, db_path):
    """
    @type db_path: str
    @module_signature: (name, features, feature_types)
    """
    super(Stamp, self).__init__(db_path, module_signature)

    self._client = bsclient.public()

  def update(self):
    """
    Get the market values and save them to the datastore
    """

    try:
      ticker = self._client.ticker()
    except Exception as e:
      sys.stderr.write(unicode(datetime.utcnow()) +
                       u'An error occurred while connecting to BitStamp: ' + unicode(e.message))

    if ticker:
      values = [datetime.utcnow()]
      values += [float(v) for v in ticker.values()]
      # Make sure that that number of features match
      values = tuple(values[:len(module_features)])
      self.insert(values)