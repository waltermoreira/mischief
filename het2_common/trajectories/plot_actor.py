import sys
import os
import subprocess
import matplotlib
matplotlib.use('Qt4Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker
from pycommon.actors.actor import Actor, ActorRef
import numpy as np

def ensure_running():
    try:
        p = ActorRef('PlotActor')
        print 'there is a plot actor'
        p.destroy_ref()
        return
    except OSError:
        print 'no plot actor, will start one'
        start_actor()
        print 'started'

class WaitActor(Actor):
    def __init__(self):
        super(WaitActor, self).__init__()
        
    def act(self):
        self.receive({'ok': lambda msg: None})
     
def start_actor():
    myself = os.path.abspath(__file__)
    if myself.endswith('.pyc'):
        myself = myself[:-1]
    w = WaitActor()
    p = subprocess.Popen(['python', myself, w.name])
    print 'launched process'
    print ' now waiting'
    w.act()
    print 'wait got ok'
    w.destroy_actor()

    
class PlotActor(Actor):

    def __init__(self):
        super(PlotActor, self).__init__('PlotActor')

    def act(self):
        try:
            while True:
                print 'About to receive'
                self.receive({'quit': self.quit,
                              'plot': self.plot})
        except StopIteration:
            return

    def _do_plot(self, data, other_data, ax, label):
        ax.plot(data)
        ax.plot(other_data)
        ax.set_xticks([])
        ax.set_title(label)

    def _do_delta_plots(self, data, other_data, ax, tol):
        ax.plot(data-other_data)
        ax.set_xticks([])
        ax.set_ylim(-tol, tol)
        
    def plot(self, msg):
        traj = np.array(msg['traj'])
        other_traj = np.array(msg['other_traj'])
        tolerances = msg['tolerances']
        fig, axs = plt.subplots(nrows=2, ncols=7)
        plt.subplots_adjust(left=0.05, right=0.95, wspace=0.3)

        labels = ['Time', 'X', 'Y', 'Z', 'Theta', 'Phi', 'Rho']
        for i, label in enumerate(labels):
            data = traj[:, i]
            other_data = other_traj[:, i]
            self._do_plot(data, other_data, axs[0, i], label)
            self._do_delta_plots(data, other_data, axs[1, i], tolerances[i])

        plt.show()
        
    def quit(self, msg):
        raise StopIteration

        
if __name__ == '__main__':
    wait_ref = sys.argv[1]
    wait = ActorRef(wait_ref)

    actor = PlotActor()

    wait.send({'tag': 'ok'})
    wait.destroy_ref()

    print 'Wait destroyed'
    reg = ActorRef('Register')
    reg.send({'tag': 'register',
              'pid': os.getpid(),
              'name': actor.name})
    reg.destroy_ref()
    print 'Actor registered'
    
    actor.act()

    actor_ref = ActorRef(actor.name)
    actor_ref.destroy_actor()
    

    