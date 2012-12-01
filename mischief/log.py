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
from .tools import pairwise
from ConfigParser import ConfigParser, NoSectionError, NoOptionError


def setup(*args):
    print 'Log with', args

