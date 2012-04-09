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
    """
    Read a file in ``point`` format to a numpy array.

    The file with name ``filename`` should be in
    ``$HET2_DEPLOY/test/trajectory_tests``.
    """
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

    The ``other`` parameters is the name of a ``point`` formatted file.
    
    Reference: wikipedia.org/wiki/Uniform_metric
    """
    c = None
    pts_flat = self.getPtsFlat(c)
    pts = np.array(pts_flat)
    # make an array of shape n x 7
    pts = pts.reshape(-1, 7)
    pts_n, _ = pts.shape
    
    other = read_trajectory(self, other_traj)
    other_n, _ = other.shape

    if pts_n != other_n:
        length_mismatch = (pts_n, other_n)
        n = min(length_mismatch)
    else:
        length_mismatch = None
        n = pts_n

    # Truncate trajectories to same length, for computing deltas
    pts = pts[:n, :]
    other = other[:n, :]
    
    # maximum values for each column
    norm = np.amax(abs(pts - other), axis=0)
    
    return {'traj': pts.tolist(),
            'other_traj': other.tolist(),
            'length_mismatch': length_mismatch,
            'distance': norm.tolist()}
    
def monkey_patch(env):
    Trajectory = env['Trajectory']
    Trajectory.uniform_distance = uniform_distance
    Trajectory.compare = uniform_distance
    Trajectory.read_trajectory = read_trajectory