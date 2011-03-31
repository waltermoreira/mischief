"""
Tools to log to screen, rolling files, and logger server.
"""

import logging
import logging.handlers
import os
import sys

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

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    LOGGER_FILE.addHandler(file_handler)
    LOGGER_FILE.setLevel(logging.DEBUG)

    LOGGER_DEBUG.addHandler(console_handler)
    LOGGER_DEBUG.setLevel(logging.DEBUG)
    
        
