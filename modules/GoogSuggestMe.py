#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sgmllib import SGMLParser
import urllib2
import urllib
import sys
from datetime import datetime
import re
from modules.Datastore import IoBucket, ModuleIoBucket


__author__ = 'Giannis Dzegoutanis'

## PychoHistory Module ########

module_signature = (("timestamp", "key", "value"), (datetime.utcnow(), "keyword", 1244000))

(module_name, module_features, example_values) = ("GoogSuggestMe",
                                                  (u'timestamp', u'key', u'value'),
                                                  (datetime.utcnow(), u'keyword', 1244000))

module_signature = (module_name, module_features, example_values)



###############################

# todo implement a module for filters. Delete FILTER_BLACKLIST global
FILTER_BLACKLIST = re.compile(u'.*thePhraseYouWantToBlackList.*')  # TODO make configurable


def filter_blacklist(row):
  return FILTER_BLACKLIST.match(row[1]) is None


def map_unicode(row):
  # _id, timestamp, keyword, value
  return row[0], unicode(row[1]), row[2]


class GoogSuggestMe:
  def __init__(self, bucket_path ):
    self._path = bucket_path

  def __enter__(self):
    self._goog_sug = Goog(self._path)
    return self._goog_sug

  def __exit__(self, type, value, traceback):
    self._goog_sug.close()


class Goog(ModuleIoBucket):
  def __init__(self, db_path):
    """
    @type db_path: str
    @module_signature: (name, features, feature_types)
    """
    super(Goog, self).__init__(db_path, module_signature)
    self.PullSuggestions = PullSuggestions

  def suggest(self, *base_queries):
    """
    Get keywords and put them to the db
    """
    for base_query in base_queries:
      if isinstance(base_query, list):
        self.suggest(*base_query)
      else:
        base_query += u' %s'
        alphabet = u'abcdefghijklmnopqrstuvwxyz'
        for letter in alphabet:
          q = base_query % letter
          try:
            self.write_suggestions_to_db(q)
          except Exception, e:
            sys.stderr.write(str(datetime.utcnow()) +
                             " An error occurred while getting some " +
                             "'google complete' results: " +
                             e.message)

  def fetch_suggestions(self, query):
    query = unicode(urllib.urlencode({u'q': query}))
    url = u'http://google.com/complete/search?output=toolbar&%s' % query
    time_now = datetime.utcnow()
    res = urllib2.urlopen(url)
    parser = self.PullSuggestions()
    parser.feed(res.read())
    parser.close()
    now_array = [time_now for _ in xrange(min(len(parser.suggestions), len(parser.queries)))]
    data = zip(now_array, parser.suggestions, parser.queries)
    data = filter(filter_blacklist, map(map_unicode, data))
    return data

  def write_suggestions_to_db(self, query):
    """ Generate keywords and search traffic values from Google """
    data = self.fetch_suggestions(query)

    self.insertmany(data)


##############################################################################
# src: http://stackoverflow.com/questions/4440139/is-there-a-google-insights-api
# Define the class that will parse the suggestion XML
class PullSuggestions(SGMLParser):
  def reset(self):
    SGMLParser.reset(self)
    self.suggestions = []
    self.queries = []

  def start_suggestion(self, attrs):
    for a in attrs:
      if a[0] == 'data':
        self.suggestions.append(a[1])

  def start_num_queries(self, attrs):
    for a in attrs:
      if a[0] == 'int':
        self.queries.append(a[1])
