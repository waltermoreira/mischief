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

Launch actor with:

    spawn(MyActor, foo=4)

Keyword arguments are set in the actor in the new process.
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
            self.name, self.pid = start_actor(self.__class__.__name__,
                                              self.__class__.__module__)
        else:
            super(ProcessActor, self).__init__(*args, **kwargs)

    def remote_init(self, msg):
        """
        Save addresses of other actors we want this process to
        know
        """
        del msg['tag']
        for name in msg:
            setattr(self, name, msg[name])
        with ActorRef(msg['reply_to']) as sender:
            sender.finished_init()
            
    def act(self):
        """
        If not overloaded, provide a basic act loop that can be
        customized through 'process_act'.
        """
        self.receive(init=self.remote_init)
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
        self.receive(ok=self.read_reply)
        return self.name, self.pid

    def read_reply(self, msg):
        self.name = msg['name']
        self.pid = msg['pid']
        
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

def spawn(actor, ref_name=None, **kwargs):
    """
    Utility function to start a process actor and initialize it
    """
    if ref_name is not None:
        with ActorRef(ref_name) as ref:
            # Do not start if it's already alive
            if ref.is_alive():
                return
    a = actor()
    class Wait(Actor):
        def act(self):
            self.receive(finished_init=None)
    with ActorRef(a.name) as ref, Wait() as wait:
        ref.init(reply_to=wait.name, **kwargs)
        wait.act()
    return a.name, a.pid
    
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
    wait.ok(name=actor.name, pid=os.getpid())
    wait.close()

    # The new process ends when the client's actor finishes its
    # ``act`` method.
    try:
        actor.act()
    except KeyboardInterrupt:
        pass
    

