import sys
import os
import subprocess
import matplotlib
matplotlib.use('Qt4Agg')
import matplotlib.pyplot as plt
from pycommon.actors.actor import Actor, ActorRef


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
                              'plot': self.plot,
                              'hi': self.say_hi})
        except StopIteration:
            return

    def plot(self, msg):
        plt.plot([1,2,3,5])
        plt.show()
        
    def quit(self, msg):
        raise StopIteration
        
    def say_hi(self, msg):
        print 'hi!!!'


if __name__ == '__main__':
    wait_ref = sys.argv[1]
    wait = ActorRef(wait_ref)

    actor = PlotActor()

    wait.send({'tag': 'ok'})
    wait.destroy_ref()

    actor.act()

    actor_ref = ActorRef(actor.name)
    actor_ref.destroy_actor()
    

    