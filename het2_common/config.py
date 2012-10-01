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
from ConfigParser import ConfigParser, NoSectionError, NoOptionError
import log

HET2_DEPLOY = None
HET2_AUXIL = None

ACTOR_MANAGER_IP = 'localhost'
ACTOR_MANAGER_PORT = 5123

def get_manager_address():
    conf_file = os.path.join(os.environ['HET2_DEPLOY'],
                             'etc/actors.conf')
    c = ConfigParser()
    res = c.read(conf_file)
    try:
        return (c.get('manager', 'host'), int(c.get('manager', 'port')))
    except (NoSectionError, NoOptionError):
        return (ACTOR_MANAGER_IP, ACTOR_MANAGER_PORT)

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

def load_config(name, mod, filename):
    """
    Update the module ''mod'' from the values of the configuration
    file ''<deploy_dir>/etc/<name>''.

    Most common use is::

      import logger.config
      load_config('logger', logger.config, __file__)

    or substituting gui for logger.
    """
    cfg_file = join(os.environ['HET2_DEPLOY'], 'etc', name+'.conf')
    cfg = ConfigParser()
    res = cfg.read(cfg_file)
    if res:
        print >>sys.stderr, ('Loaded config file: %s in %s'
                             %(cfg_file, filename))
    else:
        print >>sys.stderr, ('No config file found in %s'
                             %join(os.environ['HET2_DEPLOY'], 'etc'))
        print >>sys.stderr, 'Using defaults from module %s.config' %name
    read_config(cfg, mod)
    global stats
    stats = importlib.import_module(name+'.compilestats')
    log.setup_log(name, os.environ['HET2_DEPLOY'])
    
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
        abs_filename = join(dirname(abspath(caller.f_globals['__file__'])), filename)
        execfile(abs_filename, caller.f_globals, caller.f_locals)
    except IOError:
        print 'Local config "%s" not found. Ignoring it.' %filename
    finally:
        del caller
    
