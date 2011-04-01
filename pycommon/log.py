"""
Tools to log to screen, rolling files, and logger server.
"""

import logging
import logging.handlers
import os
import sys
import json
import itertools
import re

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return itertools.izip(a, b)

def get_dict(msg):
    """
    'foo := 5 and bar := "a b"' -> {'foo': 5
                                    'bar': 'a b'}
    """
    result = {}
    for (k, v) in pairwise(re.split(':=', msg)):
        # key is the last word before :=
        key = k.split()[-1]
        v = v.lstrip()
        c = v[0]
        if c in ('"', "'"):
            # value is a quoted string, extract it
            g = re.match(r'((?:[^%c\\]|\\.)*)' %c, v[1:])
            val = g.group(1)
        else:
            # value may be an integer, float, or word after :=
            val = v.split()[0]
            try:
                val = int(val)
            except ValueError:
                try:
                    val = float(val)
                except ValueError:
                    pass
        result[key] = val
    return result
    
class JSONFormatter(logging.Formatter):

    def __init__(self, *args, **kwargs):
        super(JSONFormatter, self).__init__(*args, **kwargs)

    def format(self, record):
        #super(JSONFormatter, self).format(record)
        return 'log ' + json.dumps(self.parse_message(record)) + '\n'

    def parse_message(self, record):
        msg = record.getMessage()
        result = get_dict(msg)
        result['message'] = msg
        result['function'] = record.funcName
        result['file'] = record.pathname
        result['line'] = record.lineno
        result['level'] = record.levelname
        return result
    
def setup_log(mod_name, deploy_dir):
    LOGGER_FILE = logging.getLogger('file')
    LOGGER_DEBUG = logging.getLogger('file.console')
    LOGGER_LOGGER = logging.getLogger('file.logger')
    LOGGER_FULL = logging.getLogger('file.full')
    
    fname = os.path.join(deploy_dir, 'log', mod_name, 'gui.log')
    file_handler = logging.handlers.RotatingFileHandler(fname)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-8s [%(filename)30s:%(lineno)-4s] - %(message)s")
    file_handler.setFormatter(formatter)

    json_formatter = JSONFormatter()
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(json_formatter)

    LOGGER_FILE.addHandler(file_handler)
    LOGGER_FILE.setLevel(logging.DEBUG)

    LOGGER_DEBUG.addHandler(console_handler)
    LOGGER_DEBUG.setLevel(logging.DEBUG)
    
    
def test():
    setup_log('gui', '/home/moreira/deploy')
    return logging.getLogger('file.console')
