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
        ActorRef('PlotActor')
        return
    except OSError:
        start_actor()

class WaitActor(Actor):
    def __init__(self):
        super(WaitActor, self).__init__(prefix='plot_actor_wait')
        
    def act(self):
        self.receive({'ok': lambda msg: None})
     
def start_actor():
    myself = os.path.abspath(__file__)
    if myself.endswith('.pyc'):
        myself = myself[:-1]
    w = WaitActor()
    p = subprocess.Popen(['python', myself, w.name])
    w.act()
    w.destroy_actor()

    
class PlotActor(Actor):

    def __init__(self):
        super(PlotActor, self).__init__('PlotActor')

    def act(self):
        try:
            while True:
                self.receive({'quit': self.quit,
                              'test': self.test,
                              'tight': self.tight,
                              'plot': self.plot})
        except StopIteration:
            return

    def test(self, msg):
        fig, axs = plt.subplots(nrows=2, ncols=7)
        axs[0,0].plot([1,2,3,5])
        plt.draw()
        print 'drawn'
        plt.show()
        print 'shown'

    def tight(self, msg):
        print 'tighting'
        plt.tight_layout(pad=0.3, w_pad=0.3, h_pad=0.3)
        
    def plot(self, msg):
        traj = np.array(msg['traj'])
        other_traj = np.array(msg['other_traj'])
        fix, axs = plt.subplots(nrows=2, ncols=7)
        plt.subplots_adjust(left=0.05, right=0.95, wspace=0.15)
        times = axs[0, 0]
        data = traj[:, 0]
        other_data = other_traj[:, 0]
        times.plot(data)
        times.plot(other_data)
        times.set_xticks([])
        #times.xaxis.set_major_locator(matplotlib.ticker.MaxNLocator(3))
        plt.show()
        print 'plotted'
        
    def quit(self, msg):
        print 'Quitting plot actor'
        raise StopIteration

        
if __name__ == '__main__':
    wait_ref = sys.argv[1]
    wait = ActorRef(wait_ref)

    actor = PlotActor()

    wait.send({'tag': 'ok'})
    wait.destroy_ref()

    actor.act()

    actor_ref = ActorRef(actor.name)
    actor_ref.destroy_actor()
    

    