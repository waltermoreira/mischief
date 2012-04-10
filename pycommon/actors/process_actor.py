"""
A base class to construct process-based actors
==============================================

Inherit from the class ``ProcessActor`` to create an actor which will
run in an independent process.

Use as::

    from pycommon.actors.process_actor import ProcessActor

    class MyActor(ProcessActor):

        def __init__(self):
            super(MyActor, self).__init__('MyActorName')

        ...

"""

from pycommon.actors.actor import Actor, ActorRef
import importlib
import os
import sys
import subprocess

class ProcessActor(Actor):

    # When creating an instance, the first time launch a new Python
    # process. Subsequent times (from inside the new process) just
    # create a regular instance.
    launch = True
    
    def __init__(self, *args, **kwargs):
        super(ProcessActor, self).__init__(*args, **kwargs)
        if self.launch:
            start_actor(self.__class__.__name__,
                        self.__class__.__module__)

class WaitActor(Actor):
    """
    Actor to wait for a message from the client's class saying that
    it started ok.
    """
    
    def __init__(self):
        super(WaitActor, self).__init__()
        
    def act(self):
        self.receive({'ok': lambda msg: None})
     
def start_actor(name, module):
    """
    Start a new Python subprocess.

    We pass the information of the client's object to instantiate, and
    the waiting actor to confirm that everything went well on
    startup.
    """
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
    # Signal the base class ``ProcessActor`` to not start a new
    # subprocess (we are already in it!)
    cls.launch = False
    actor = cls()
    
    # Tell parent to keep going
    wait.send({'tag': 'ok'})
    wait.destroy_ref()

    # The new process ends when the client's actor finishes its
    # ``act`` method.
    actor.act()

    actor_ref = ActorRef(actor.name)
    actor_ref.destroy_actor()
    

