import pygsl.roots
import pygsl.errno

class GSLException(Exception):
    pass
    
def fdf_root(f, df, fdf, seed,
             method='steffenson', params=None,
             max_iter=100, eps_abs=0.01, eps_rel=0.0):
    """
    Find a root for the function `f`
    """
    function = pygsl.roots.gsl_function_fdf(f, df, fdf, params)
    solver_method = getattr(pygsl.roots, method)
    solver = solver_method(function)
    root = seed
    solver.set(root)

    for i in range(max_iter):
        status = solver.iterate()
        if status != pygsl.errno.GSL_SUCCESS:
            raise GSLException("solver.iterate returned %s" %status)
        prev = root
        root = solver.root()
        status = pygsl.roots.test_delta(root, prev, eps_abs, eps_rel)
        if status == pygsl.errno.GSL_SUCCESS:
            return root

    raise GSLException('solver exceeded number of iterations')

