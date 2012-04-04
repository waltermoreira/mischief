import types
from het2_common import accept_context
import numpy as np
from itertools import *


@accept_context
def _read_traj_file(filename):
    """
    Read a trajectory from a file with ``point`` format.

    Return an numpy array of dimension n x 7.
    """
    pass
    
@accept_context
def uniform_distance(self, other_traj):
    """
    Compute the uniform distance between two trajectories,
    elementwise::

        uniform_distance(self, other) -> (d1, d2, ..., d7)

    where d_i is the uniform distance in the i-th coordinate.

    The ``other_traj`` is an iterable whose elements are iterables of
    7 elements, in the order::
    
        (Ha, X, Y, Z, Theta, Phi, Rho)
    
    Reference: wikipedia.org/wiki/Uniform_metric
    """
    c = None
    print 'self is', self
    pts_flat = self.getPtsFlat(c)
    pts = np.array(pts_flat)
    # make an array of shape n x 7
    pts = pts.reshape(-1, 7)
    return pts
    
def monkey_patch(env):
    Trajectory = env['Trajectory']
    Trajectory.uniform_distance = uniform_distance
