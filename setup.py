#!/usr/bin/env python

from distutils.core import setup

setup(name='het2_common',
      version='1.0',
      description='Common utility functions to use in scripts and recipes.',
      author='Walter Moreira',
      author_email='moreira@astro.as.utexas.edu',
      packages=['het2_common',
                'het2_common.time'],
      )
