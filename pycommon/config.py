"""
:mod:`hetcommon.config` -- Handling config files
================================================

A thin wrapper around ``ConfigParser`` to handle default values for
options.  The module :mod:`~logger.config` contains the default values
for the options which are updated from the config files.
"""

import sys
import os
import importlib
import inspect
from os.path import dirname, join, abspath
from ConfigParser import ConfigParser
import log

HET2_DEPLOY = None
HET2_AUXIL = None

def read_config(cfg, mod):
    """
    Update the module ''mod'' from the values of the
    ''ConfigParser'' object ''cfg''.
    
    An option ''opt'' under a section ''sect'' updates the value of
    the attribute ''sect_opt'' in the module ''mod''.  The type of the
    attribute is determined from the original type in the module.
    """
    conversions = {int: cfg.getint,
                   float: cfg.getfloat,
                   bool: cfg.getboolean}
    for section in cfg.sections():
        for option in cfg.options(section):
            attr = '%s_%s' %(section, option)
            default = getattr(mod, attr)
            f = conversions.get(type(default), cfg.get)
            setattr(mod, attr, f(section, option))

def load_config(name, mod):
    """
    Update the module ''mod'' from the values of the configuration
    file ''<deploy_dir>/etc/<name>''.

    Most common use is::

      import logger.config
      load_config('logger', logger.config)

    or substituting gui for logger.
    """
    cfg_file = join(HET2_DEPLOY, 'etc', name+'.conf')
    cfg = ConfigParser()
    res = cfg.read(cfg_file)
    if res:
        print >>sys.stderr, 'Loaded config file:', cfg_file
    else:
        print >>sys.stderr, 'No config file found in', join(HET2_DEPLOY, 'etc')
        print >>sys.stderr, 'Using defaults from module %s.config' %name
    read_config(cfg, mod)
    global stats
    stats = importlib.import_module(name+'.compilestats')
    log.setup_log(name, HET2_DEPLOY)
    
def setup(fname):
    global HET2_DEPLOY, HET2_AUXIL
    
    HET2_DEPLOY = abspath(join(dirname(abspath(fname)), '../'))
    try:
        HET2_AUXIL = os.environ['HET2_AUXIL']
    except KeyError:
        error('Environment variable HET2_AUXIL must point to the auxil directory')

def display_compile_stats():
    print >>sys.stderr,  '****************************************************'
    print >>sys.stderr,  '* Source         - %s' %os.path.abspath(sys.argv[0])
    print >>sys.stderr,  '* Compile_branch - %s' %stats.compiled_git_branch
    print >>sys.stderr,  '* Compile_sha    - %s' %stats.compiled_git_sha
    print >>sys.stderr,  '* Compile_user   - %s' %stats.compiled_user
    print >>sys.stderr,  '* Compile_host   - %s' %stats.compiled_host
    print >>sys.stderr,  '* Compile_date   - %s' %stats.compiled_date
    print >>sys.stderr,  '****************************************************'

def use_local_config(filename):
    """
    Execute/source/inline a Python file in the current scope.
    """
    if not filename.endswith('.py'):
        filename += '.py'
    try:
        caller = inspect.stack()[1][0]
        execfile(filename, caller.f_globals, caller.f_locals)
    except IOError:
        print 'Local config "%s" not found. Ignoring it.' %filename
    finally:
        del caller
    
