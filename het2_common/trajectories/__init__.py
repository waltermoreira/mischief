import os
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
    result = []
    with open(filename) as f:
        # skip first 5 lines of header
        [next(f) for _ in range(5)]
        for line in f:
            result.append([float(x) for x in line.split()[:7]])
    return np.array(result)

@accept_context
def read_trajectory(self, filename):
    traj_file = os.path.join(os.environ['HET2_DEPLOY'],
                             'test/trajectory_tests', filename)
    return _read_traj_file(traj_file)
    
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
    pts_flat = self.getPtsFlat(c)
    pts = np.array(pts_flat)
    # make an array of shape n x 7
    pts = pts.reshape(-1, 7)
    return pts
    
def monkey_patch(env):
    Trajectory = env['Trajectory']
    Trajectory.uniform_distance = uniform_distance
    Trajectory.read_trajectory = read_trajectory