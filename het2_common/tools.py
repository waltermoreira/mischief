import itertools

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