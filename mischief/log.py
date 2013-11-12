"""
Tools to log to screen and rolling files
"""

import logging
import logging.handlers
import os
import errno
import sys
import json
import itertools
import re
import inspect
from .tools import pairwise

formatter = logging.Formatter(
    fmt='%(asctime)s %(levelname)s %(module)s:%(lineno)s %(message)s')

def setup(**args):
    destinations = args['to']
    if isinstance(destinations, str):
        destinations = [destinations]
    try:
        outer_frame = inspect.stack()[1][0]
        mod_name = args.get('module', outer_frame.f_globals['__name__'])
        filename = args.get('filename', '{}.log'.format(mod_name))
        directory = args.get('directory', '/tmp')
        logger = logging.getLogger(mod_name)
        logger.setLevel(logging.DEBUG)
        del logger.handlers[:]
        handlers = {
            'file': logging.handlers.RotatingFileHandler(
                os.path.join(directory, filename)),
            'console': logging.StreamHandler()
        }
        for dest in destinations:
            handler = handlers[dest]
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
    finally:
        del outer_frame
        

