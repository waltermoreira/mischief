import types
from het2_common import accept_context

@accept_context
def trajectory(x):
    print 'traj', x

@accept_context
def new_meth(self, x):
    print "A method of", self, x

def monkey_patch(env):
    Trajectory = env['Trajectory']
    Trajectory.new_meth = types.MethodType(new_meth, Trajectory)