"""
Tools to log to screen, rolling files, and logger server.
"""

import logging
import logging.handlers
import os
import errno
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

def parse_message(record):
    msg = record.getMessage()
    result = get_dict(msg)
    result['message'] = msg
    result['function'] = record.funcName
    result['file'] = record.pathname
    result['line'] = record.lineno
    result['level'] = record.levelname
    return result

def make_log_command(record):
    return ('log ' +
            json.dumps(parse_message(record)) +
            '\n')
    
class JSONSocketHandler(logging.handlers.SocketHandler):

    def makePickle(self, record):
        return make_log_command(record)
    
class JSONFormatter(logging.Formatter):

    def format(self, record):
        return make_log_command(record)
    
def setup_log(mod_name, deploy_dir):
    LOGGER_FILE = logging.getLogger('file')
    LOGGER_DEBUG = logging.getLogger('file.console')
    LOGGER_LOGGER = logging.getLogger('file.logger')
    LOGGER_FULL = logging.getLogger('file.console.logger')

    path = os.path.join(deploy_dir, 'log', mod_name)
    try:
        os.makedirs(path)
    except os.error as exc:
        # ignore error if dir already exists
        if exc.errno != errno.EEXIST:
            raise
    fname =  os.path.join(path, mod_name+'.log')

    file_handler = logging.handlers.RotatingFileHandler(fname)
    console_handler = logging.StreamHandler(sys.stdout)
    socket_handler = JSONSocketHandler('localhost', 8070)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-8s "
        "[%(filename)30s:%(lineno)-4s] "
        "- %(message)s")
    json_formatter = JSONFormatter()

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    LOGGER_FILE.addHandler(file_handler)
    LOGGER_FILE.setLevel(logging.DEBUG)

    LOGGER_DEBUG.addHandler(console_handler)
    LOGGER_DEBUG.setLevel(logging.DEBUG)

    LOGGER_LOGGER.addHandler(socket_handler)
    LOGGER_LOGGER.setLevel(logging.DEBUG)
    
def test(s):
    setup_log('gui', '/home/moreira/deploy')
    return logging.getLogger(s)
