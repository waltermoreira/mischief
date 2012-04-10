from pycommon.actors.actor import Actor, ActorRef
import importlib
import os
import sys
import subprocess

class ProcessActor(Actor):

    launch = True
    
    def __init__(self, *args, **kwargs):
        super(ProcessActor, self).__init__(*args, **kwargs)
        if self.launch:
            start_actor(self.__class__.__name__,
                        self.__class__.__module__)

class WaitActor(Actor):

    def __init__(self):
        super(WaitActor, self).__init__()
        
    def act(self):
        self.receive({'ok': lambda msg: None})
     
def start_actor(name, module):
    myself = os.path.abspath(__file__)
    if myself.endswith('.pyc'):
        myself = myself[:-1]
    w = WaitActor()
    p = subprocess.Popen(['python', myself, w.name, name, module])
    w.act()
    w.destroy_actor()

if __name__ == '__main__':
    wait_ref = sys.argv[1]
    wait = ActorRef(wait_ref)

    actor_class = sys.argv[2]
    actor_module = sys.argv[3]
    mod = importlib.import_module(actor_module)
    cls = getattr(mod, actor_class)
    cls.launch = False
    actor = cls()
    print 'actor created', actor
    
    wait.send({'tag': 'ok'})
    wait.destroy_ref()

    print 'ok sent, acting...'
    actor.act()
    print 'act finished'
    actor_ref = ActorRef(actor.name)
    actor_ref.destroy_actor()
    

