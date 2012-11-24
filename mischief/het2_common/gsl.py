"""
Simple helper functions to reuse the tedious loops we need to write
for GSL functions.
"""

import pygsl.roots
import pygsl.multiroots
import pygsl.errno
import pygsl.minimize
import pygsl.multiminimize
import numpy

class GSLException(Exception):
    pass
    
def fdf_root(f, df, fdf, seed,
             method='steffenson', params=None,
             max_iter=100, eps_abs=0.01, eps_rel=0.0):
    """
    Find a root for the function `f`, using derivatives
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

def f_multi_root(f, n, seed, method='hybrids', params=None,
                 max_iter=100, eps_abs=0.01):
    """
    Find a root for the system of equations `f = 0`, of `n`
    equations and `n` unknowns.
    """
    system = pygsl.multiroots.gsl_multiroot_function(f, params, n)
    solver_method = getattr(pygsl.multiroots, method)
    solver = solver_method(system, n)
    solver.set(numpy.array(seed))

    for i in range(max_iter):
        status = solver.iterate()
        print solver.getx(), solver.getf()
        if status != pygsl.errno.GSL_SUCCESS:
            raise GSLException("solver.iterate returned %s" %status)
        y = solver.getf()
        status = pygsl.multiroots.test_residual(y, eps_abs)
        if status == pygsl.errno.GSL_SUCCESS:
            return solver.getx()

    raise GSLException('solver exceeded number of iterations')
    
    
def multi_minimize(f, n, seed, steps,
                   params=None,
                   max_iter=100, eps=0.01):
    """
    Find a minimum for the function `f` in `n` variables
    """
    system = pygsl.multiminimize.gsl_multimin_function(f, params, n)
    solver = pygsl.multiminimize.nmsimplex(system, n)

    start_point = seed
    initial_steps = steps
    solver.set(start_point, initial_steps)

    for i in range(max_iter):
        status = solver.iterate()
        if status != pygsl.errno.GSL_SUCCESS:
            raise GSLException("solver.iterate returned %s" %status)
        status = pygsl.multiminimize.test_size(solver.size(), eps)
        if status == pygsl.errno.GSL_SUCCESS:
            return solver.getx()

    raise GSLException('solver exceeded number of iterations')

def minimize(f, m, a, b, params=None, max_iter=100, eps_abs=0.01, eps_rel=0.0):
    """
    Find minimum of a function in one variable
    """
    system = pygsl.minimize.gsl_function(f, params)
    minimizer = pygsl.minimize.brent(system)
    minimizer.set(m, a, b)

    for i in range(max_iter):
        status = minimizer.iterate()
        if status != pygsl.errno.GSL_SUCCESS:
            raise GSLException('minimizer.iterate returned %s' %status)
        a = minimizer.x_lower()
        b = minimizer.x_upper()
        m = minimizer.minimum()
        status = pygsl.minimize.test_interval(a, b, eps_abs, eps_rel)
        if status == pygsl.errno.GSL_SUCCESS:
            return m

    raise GSLException('minimizer exceeded number of iterations')