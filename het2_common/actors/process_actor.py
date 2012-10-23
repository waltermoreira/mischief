"""
A base class to construct process-based actors
==============================================

Inherit from the class ``ProcessActor`` to create an actor which will
run in an independent process.

Use as::

    from het2_common.actors.process_actor import ProcessActor

    class MyActor(ProcessActor):

        def __init__(self):
            super(MyActor, self).__init__('MyActorName')

        ...

"""

from het2_common.actors.actor import Actor, ActorRef
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
        if self.launch:
            self.name = start_actor(self.__class__.__name__,
                                    self.__class__.__module__)
        else:
            super(ProcessActor, self).__init__(*args, **kwargs)

    def act(self):
        """
        If not overloaded, provide a basic act loop that can be
        customized through 'process_act'.
        """
        try:
            while True:
                self.process_act()
        except StopIteration:
            return

    def process_act(self):
        raise NotImplementedError
        
class WaitActor(Actor):
    """
    Actor to wait for a message from the client's class saying that
    it started ok.
    """
    
    def __init__(self):
        super(WaitActor, self).__init__()
        
    def act(self):
        self.receive(ok=self.read_value('name'))
        return self.name
     
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
    with WaitActor() as w:
        p = subprocess.Popen(['python', myself, w.name, name, module])
        return w.act()

if __name__ == '__main__':
    wait_ref = sys.argv[1]
    wait = ActorRef(wait_ref)

    actor_class = sys.argv[2]
    actor_module = sys.argv[3]
    print 'will import', actor_module
    mod = importlib.import_module(actor_module)
    cls = getattr(mod, actor_class)
    # Signal the base class ``ProcessActor`` to not start a new
    # subprocess (we are already in it!)
    cls.launch = False
    actor = cls()
    
    # Tell parent to keep going
    wait.ok(name=actor.name)
    wait.close()

    # The new process ends when the client's actor finishes its
    # ``act`` method.
    print 'Process actor will act'
    actor.act()
    print 'Process actor stopped acting, will quit!'
    

