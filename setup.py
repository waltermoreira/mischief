#!/usr/bin/env python

import os
from distutils.core import setup, Extension

COMMON_DIR = os.path.join(os.environ['HET2_WKSP'], 'common')
AUXIL_DIR = os.environ['HET2_AUXIL']
HET2_LIBS = os.environ['HET2_LIBS']

het2_time = Extension(
      'het2_common.time._het2_time',
      sources=['het2_common/time/het2_time_wrap.cxx', os.path.join(COMMON_DIR, 'src/HET2_Time.cpp')],
      include_dirs=[os.path.join(COMMON_DIR, 'include'), HET2_LIBS],
      library_dirs=[os.path.join(HET2_LIBS, 'libtime')],
      libraries=['time', 'm', 'rt']
      )

het2_cjson = Extension(
      'het2_common.cjson._het2_cjson',
      sources=['het2_common/cjson/het2_cjson_wrap.cxx', os.path.join(AUXIL_DIR, 'cJSON/cJSON.c')],
      include_dirs=[os.path.join(AUXIL_DIR, 'cJSON')],
      library_dirs=[],
      libraries=[]
      )

setup(name='het2_common',
      version='1.0',
      description='Common utility functions to use in scripts and recipes.',
      author='Walter Moreira',
      author_email='moreira@astro.as.utexas.edu',
      ext_modules=[het2_time, het2_cjson],
      packages=['het2_common',
                'het2_common.time',
                'het2_common.trajectories',
                'het2_common.astronomy',
                'het2_common.cjson'],
      )
