#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import datetime
import logging
import os
import sys

from twitter import TwitterStream, OAuth, Twitter, oauth_dance, read_token_file
import yaml
from dateutil import parser as dateutilp

from modules.Datastore import ModuleIoBucket


__author__ = 'Giannis Dzegoutanis'

## PychoHistory Module ########

module_name = u'TweetCatcher'
module_features = (u'tweet_id', u'timestamp', u'tweet_id_str', u'lang', u'text')
example_values = (342066498631770113L, datetime.utcnow(), u'342066498631770113', u'en', u'Tweet text')

module_signature = (module_name, module_features, example_values)

###############################

log = logging.getLogger(u'TweetCatcher')
basedir = os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

oauth_filename = os.path.join(basedir, u'PychoHistoryDatastore', u'tweet-catcher-oauth.keys')
settings_filename = os.path.join(basedir, u'settings.yaml')

try:
  settings = yaml.load(open(settings_filename))
except IOError as e:
  settings = dict(oauth_consumer_credentials=dict(secret=u'', key=u''))
  log.warning(u'Could not find a settings.yaml file.')

OAUTH = (settings['oauth_consumer_credentials']['key'],
         settings['oauth_consumer_credentials']['secret'])


class TweetCatcher(object):
  def __init__(self, bucket_path, oauth_consumer_credentials=OAUTH):
    self._path = bucket_path
    self._oauth = oauth_consumer_credentials

  def __enter__(self):
    self._tweet = Tweet(self._path, self._oauth)
    return self._tweet

  def __exit__(self, type, value, traceback):
    self._tweet.close()


class Tweet(ModuleIoBucket):
  def __init__(self, db_path, oauth_consumer_credentials):
    """
    @type db_path: str
    @module_signature: (name, features, feature_types)
    """
    super(Tweet, self).__init__(db_path, module_signature)

    self._oauth_consumer = oauth_consumer_credentials
    key, secret = self._oauth_consumer

    if not os.path.exists(oauth_filename):
      oauth_dance(u'Chitsapp', key, secret, oauth_filename)

    self._setup_twitter_stream()
    self._setup_twitter()

  def _oath_tokens(self):
    oauth_token, oauth_token_secret = read_token_file(oauth_filename)
    return oauth_token, oauth_token_secret

  def _get_oauth(self):
    oauth_token, oauth_token_secret = self._oath_tokens()
    consumer_key, consumer_secret = self._oauth_consumer
    oauth = OAuth(oauth_token, oauth_token_secret, consumer_key, consumer_secret)
    return oauth

  def _setup_twitter_stream(self):
    self._twitter_stream = TwitterStream(auth=self._get_oauth())

  def _setup_twitter(self):
    self._twitter = Twitter(auth=self._get_oauth())

  def _insert_tweet(self, tweet):
    try:
      self.insert(tweet2tuple(tweet))
    except Exception, e:
      sys.stderr.write(unicode(datetime.utcnow()) +
                       u'An error occurred while inserting some tweets: ' + unicode(e.message))

  def _create_table_query(self):
    '''
    Override the default create_table_query to make the tweet-id the primary key
    '''
    (name, features, types) = self._module_signature
    types_query = ", ".join([" ".join(v) for v in zip(features[1:], types[1:])])
    return u" CREATE TABLE %s ( %s INTEGER PRIMARY KEY, %s ) " % (name, features[0], types_query)

  def _create_insert_query(self):
    num_of_features = len(self._features())
    features_query = ", ".join(["?" for _ in xrange(num_of_features)])
    return u" INSERT OR IGNORE INTO %s VALUES( %s ) " % (self._name(), features_query)

  def _max_id(self):
    '''
    Query the datastore for the MaxId.
    '''
    # Get the minimum of the tweet_id feature
    max_id = self._fetchone(u"SELECT MAX(%s) FROM %s" % (module_features[0], self._name()))

    if len(max_id) == 1:
      return max_id[0]
    else:
      return 0

  def next_results_params(self, search_metadata):
    if search_metadata and 'next_results' in search_metadata:
      next_results = search_metadata['next_results'][1:].split('&')
      params = dict(tuple(p.split('=')) for p in next_results)
      return params

  def search(self, query):
    log.debug(u'Search query is ' + query)
    log.debug(u'Max id is %s' % self._max_id())
    search_params = dict(q=query, count=100, include_entities=True, since_id=self._max_id())

    while search_params:
      reply = self._twitter.search.tweets(**search_params)

      for tweet in reply['statuses']:
        log.info(u'%s - %s - %s - %s - "%s"' % tweet2tuple(tweet))
        self._insert_tweet(tweet)

      search_params = self.next_results_params(reply['search_metadata'])

  def start_stream(self, query):
    log.debug(u'Stream query is ' + query)
    iterator = self._twitter_stream.statuses.filter(track=query)

    for tweet in iterator:
      if 'limit' in tweet:
        log.warn(u'Lost %s messages' % tweet['limit']['track'])
      else:
        log.info(u'%s - %s - %s - %s - "%s"' % tweet2tuple(tweet))
        self._insert_tweet(tweet)


def tweet2tuple(tweet):
  '''
  Convert a JSON dict to a tweet tuple.  If you want to include more or less data, this is where you can change it.
  '''

  try:
    # Clean up the text field
    clean_text = tweet['text'].replace("\r", " ").replace("\n", " ").replace("\t", " ")
    # Replace the original URLs
    if 'entities' in tweet:
      for url in tweet['entities']['urls']:
        clean_text = clean_text.replace(url['url'], url['expanded_url'])

    # Convert to tuple
    tweet_tuple = (
      tweet['id'],
      dateutilp.parse(tweet['created_at']),
      tweet['id_str'],
      tweet['lang'],
      clean_text
    )
  except:
    raise Exception('Could not decode tweet %s' % tweet)

  return tweet_tuple