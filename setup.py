#!/usr/bin/env python

import os
from setuptools import setup
import redis

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = 'redis',
    description = 'A Python client library for the redis key value database',
    long_description = read('readme.txt'),
    license = 'BSD',
    version = redis.__version__,
    author = redis.__author__,
    author_email = redis.__email__,
    url = '',
    keywords = 'redis k-v database',
    classifiers = [
        ],
    py_modules = ['redis.redis', 'redis.helpers'],
    platforms='any',
    )
