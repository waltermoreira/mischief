import itertools
import sys
import os
from contextlib import contextmanager

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return itertools.izip(a, b)

def scanl(f, iterable):
    """
    (x0, x1, x2, ...) -> (x0, f(x0, x1), f(f(x0, x1), x2), ...)
    """
    prev = None
    for x in iterable:
        prev = f(prev, x) if prev is not None else x
        yield prev

@contextmanager
def no_stdout():
    """
    Context manager to temporarily disable stdout
    """
    old_stdout = sys.stdout
    try:
        old_stdout.flush()
        sys.stdout = open(os.devnull, 'w')
        yield
    finally:
        sys.stdout = old_stdout

@contextmanager
def no_stderr():
    """
    Context manager to temporarily disable stdout
    """
    old_stderr = sys.stderr
    try:
        old_stderr.flush()
        sys.stderr = open(os.devnull, 'w')
        yield
    finally:
        sys.stderr = old_stderr

class Addressable(object):
    """Interface for addressable objects."""

    def address(self):
        raise NotImplementedError
