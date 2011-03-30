"""
Tools to log to screen, rolling files, and logger server.
"""

import logging
import logging.handlers
import os

LOGGER = logging.getLogger('HET2Logger')

def setup_log(mod_name, deploy_dir):
    LOGGER.setLevel(logging.DEBUG)
    fname = os.path.join(deploy_dir, 'log', mod_name, 'gui.log')
    file_handler = logging.handlers.RotatingFileHandler(fname)
    LOGGER.addHandler(file_handler)
        
        
def LOG(message):
    LOGGER.debug('foo')
