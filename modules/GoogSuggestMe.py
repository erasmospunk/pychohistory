#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sgmllib import SGMLParser
import urllib2
import urllib
import sys
from datetime import datetime
import re


__author__ = 'Giannis Dzegoutanis'


# todo implement a module for filters. Delete FILTER_BLACKLIST global
FILTER_BLACKLIST = re.compile(u'.*thePhraseYouWantToBlackList.*')  # TODO make configurable


def filter_blacklist(row):
  return FILTER_BLACKLIST.match(row[1]) is None


def map_unicode(row):
  # _id, timestamp, keyword, value
  return row[0], unicode(row[1]), row[2]


class GoogSuggestMe:
  def __init__(self, io_bucket ):
    self._io_bucket = io_bucket

  def __enter__(self):
    self._goog_sug = Goog(self._io_bucket)
    return self._goog_sug

  def __exit__(self, type, value, traceback):
    pass


class Goog:
  def __init__(self, io_bucket):
    self._io_bucket = io_bucket
    self.PullSuggestions = PullSuggestions

  def suggest(self, base_query):
    """
    Get keywords and put them to the db
    """
    base_query += u' %s'
    alphabet = u'abcdefghijklmnopqrstuvwxyz'
    for letter in alphabet:
      q = base_query % letter
      try:
        self.write_suggestions_to_db(q)
      except Exception, e:
        sys.stderr.write(str(datetime.utcnow()) +
                         " An error occured while getting some " +
                         "'google complete' results: " +
                         e.message)

  def write_suggestions_to_db(self, q):
    """ Generate keywords and search traffic values from Google """
    query = unicode(urllib.urlencode({u'q': q}))
    url = u'http://google.com/complete/search?output=toolbar&%s' % query
    time_now = datetime.utcnow()

    res = urllib2.urlopen(url)
    parser = self.PullSuggestions()
    parser.feed(res.read())
    parser.close()

    now_array = [time_now for _ in xrange(min(len(parser.suggestions), len(parser.queries)))]

    data = zip(now_array, parser.suggestions, parser.queries)
    data = filter(filter_blacklist, map(map_unicode, data))
    self._io_bucket.write(data)


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
