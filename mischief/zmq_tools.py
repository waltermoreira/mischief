from contextlib import contextmanager

import zmq


Context = zmq.Context()


@contextmanager
def zmq_socket(zmq_type):
    """Context manager for zmq sockets.

    Use as::

        with zmq_socket(zmq.REP) as s:
            ...
    
    """
    s = Context.socket(zmq_type)
    yield s
    s.close()
