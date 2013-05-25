#!/usr/bin/env python
# -*- coding: utf-8 -*-
from distutils.core import setup

setup(
  name='PychoHistory',
  version='1',
  packages=['pychohistory'],
  url='http://erasmospunk.github.io/pychohistory/',
  license='MIT',
  author='Giannis Dzegoutanis',
  author_email='kamil.madac@gmail.com',
  description='A datastore utility that gathers data from different web sources',
  requires=['requests']
)
