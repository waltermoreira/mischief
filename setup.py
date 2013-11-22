#!/usr/bin/env python

import os
from setuptools import setup

setup(name='mischief',
      version='0.1.0',
      description='Library for Erlang-like actors.',
      author='Walter Moreira',
      author_email='walter@waltermoreira.net',
      packages=['mischief',
                'mischief.test'],
      license='LICENSE.txt',
      long_description=open('README.rst').read(),
      install_requires=[
          "pyzmq >= 2.1.12",
          "pytest >= 2.4",
          "flexmock"
      ]
)
