"""
Tools to log to screen and rolling files.

Use as::

    logger = setup(to='console')
    logger = setup(to=['file', 'console'])

    logger.debug('a message')

"""

import logging
import logging.handlers
import os
import inspect


formatter = logging.Formatter(
    fmt='%(asctime)s %(levelname)s %(module)s:%(lineno)s %(message)s')


def setup(**args):
    """
    Keyword arguments are::

        to: 'console' or 'file' [mandatory]
        module: module name (default to module where setup is used)
        filename: (default to module name, no extension)
        directory: (default to /tmp)
    
    """
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


def show_msg(msg, width=50, indent=0):
    """Pretty print message"""

    msg = dict(msg)
    fmt = "{}:\n{}"
    tag = msg.pop('tag', 'timed out')
    fields = '\n'.join('    {} = {}'.format(k, msg[k])
                       for k in sorted(msg.keys()))
    return fmt.format(tag, fields)
