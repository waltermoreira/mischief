from functools import wraps

def accept_context(f):
    """
    A decorator to discard the context object in Python functions.

    Use as::

        @accept_context
        def f(x, y):
            ...

    Then, the function ``f`` can be called as ``f(x, y)`` or ``f(x, y,
    c)``, where ``c`` is a context object.
    """
    import _TCS
    Context = type(_TCS.get_context())
    @wraps(f)
    def wrapper(*args, **kwargs):
        if len(args) >= 1 and type(args[-1]) is Context:
            args = args[:-1]
        return f(*args, **kwargs)
    return wrapper
