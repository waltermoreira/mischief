import os
import types
from het2_common import accept_context
import numpy as np
from itertools import *
from ..trajectories import plot_actor
from pycommon.actors.actor import ActorRef

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
def read_trajectory(filename):
    """
    Read a file in ``point`` format to a numpy array.

    The file with name ``filename`` should be in
    ``$HET2_DEPLOY/test/trajectory_tests``.
    """
    traj_file = os.path.join(os.environ['HET2_DEPLOY'],
                             'test/trajectory_tests', filename)
    return _read_traj_file(traj_file)
    
@accept_context
def compare_trajectories(traj, traj_file, plots=True, tolerances=None):
    plot_actor.ensure_running()
    
    traj_pts = np.reshape(np.array(traj.getPtsFlat(None)), (-1, 7))
    traj_pts_n, _ = traj_pts.shape

    other_pts = read_trajectory(traj_file)
    other_pts_n, _ = other_pts.shape

    print
    
    if traj_pts_n != other_pts_n:
        n = min(traj_pts_n, other_pts_n)
        print 'There is a length mismatch in trajectories:'
        print '    trajectory object: %s points' %traj_pts_n
        print '    trajectory file  : %s points' %other_pts_n
        print 'Will consider only %s points from each trajectory.' %n
    else:
        n = traj_pts_n
        print 'Trajectory length: %s points' %n

    # Truncate trajectories to same length, for computing deltas
    traj_pts = traj_pts[:n, :]
    other_pts = other_pts[:n, :]

    # maximum values for each column
    norm = np.amax(abs(traj_pts - other_pts), axis=0)
    rms = np.sqrt(np.sum((traj_pts - other_pts)**2, axis=0)/n)

    print
    
    for i, label in enumerate(['Time', 'X', 'Y', 'Z', 'Theta', 'Phi', 'Rho']):
        print '%-5s: dist = %7.3f, rms = %7.3f' %(label, norm[i], rms[i])
        
    # send data to plot actor
    if plots:
        pa = ActorRef('PlotActor')
        pa.send({'tag': 'plot',
                 'traj': traj_pts.tolist(),
                 'other_traj': other_pts.tolist()})
        pa.destroy_ref()
    
