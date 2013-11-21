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

import sys
import os
import importlib
import subprocess

# Add mischief package to sys.path, so the python subprocess can find
# this same file
sys.path.append(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../'))

from mischief.actors.actor import Actor, ActorRef
from mischief.exceptions import ActorFinished, SpawnTimeoutError

class ProcessActor(Actor):

    # When creating an instance, the first time launch a new Python
    # process. Subsequent times (from inside the new process) just
    # create a regular instance.
    launch = True
    
    def __init__(self, *args, **kwargs):
        if self.launch:
            self.remote_addr, self.pid = start_actor(self.__class__.__name__,
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
        except (StopIteration, ActorFinished):
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
        self.success = True
        self.receive(
            ok = self.read_reply,
            timed_out = lambda _: setattr(self, 'success', False),
            timeout = 5
        )
        if not self.success:
            raise SpawnTimeoutError("did not get 'ok' from subprocess")
        return self.spawn_address, self.pid

    def read_reply(self, msg):
        self.spawn_address = msg['spawn_address']
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
        w_name, _, _ = w.address()
        p = subprocess.Popen(['python', myself, w_name, name, module])
        return w.act()

class SpawnTimeoutError(Exception):
    pass
    
def spawn(actor, name=None, **kwargs):
    """
    Utility function to start a process actor and initialize it
    """
    if name is not None:
        with ActorRef((name, 'localhost', None)) as ref:
            # Do not start if it's already alive
            if ref.is_alive():
                return ref.full_address()
    a = actor()

    class Wait(Actor):
        def act(self):
            self.success = True
            self.receive(
                finished_init = None,
                timed_out = lambda _: setattr(self, 'success', False),
                timeout = 5)
            return self.success

    with ActorRef(a.remote_addr) as ref, Wait() as wait:
        ref.init(reply_to=wait, **kwargs)
        if wait.act():
            return a.remote_addr, a.pid
        else:
            raise SpawnTimeoutError('failed to init remote process')

class PEcho(ProcessActor):

    def __init__(self):
        super(PEcho, self).__init__()

    def process_act(self):
        self.receive(
            _ = self.do_pecho
        )

    def do_pecho(self, msg):
        print('Process Echo:')
        print(msg)

    
if __name__ == '__main__':
    _, wait_name, actor_class, actor_module = sys.argv
    mod = importlib.import_module(actor_module)
    cls = getattr(mod, actor_class)
    # Signal the base class ``ProcessActor`` to not start a new
    # subprocess (we are already in it!)
    cls.launch = False
    actor = cls()
    
    with ActorRef(wait_name) as wait:
        # Tell parent to keep going
        wait.ok(spawn_address=actor.address(), pid=os.getpid())

    # The new process ends when the client's actor finishes its
    # ``act`` method.
    try:
        actor.act()
    except KeyboardInterrupt:
        pass
    

