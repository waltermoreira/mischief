#!/usr/bin/env python

from setuptools import setup, find_packages


setup(name='mischief',
      version='0.1.0',
      description='Library for Erlang-like actors.',
      url='http://github/waltermoreira/mischief',
      author='Walter Moreira',
      author_email='walter@waltermoreira.net',
      packages=find_packages(),
      license='GPLv3',
      long_description=open('README.rst').read(),
      install_requires=[
          "pyzmq >= 2.1.12",
          "pytest >= 2.4",
          "flexmock"
      ]
)
