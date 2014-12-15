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
sys.path.insert(
    0, os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../')))

from mischief.actors.actor import Actor, ActorRef
from mischief.exceptions import ActorFinished, SpawnTimeoutError, PipeException
from mischief.tools import Addressable

class ProcessActorProxy(Addressable):
    """A proxy to work as a context manager for the process actors."""

    def __init__(self, address, pid):
        self._address = address
        self.pid = pid

    def address(self):
        return self._address

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Actor context closes it on exit
        """
        try:
            with ActorRef(self.address()) as myself:
                myself.close_actor()
        except PipeException:
            # If actor was already closed, ignore error from the
            # reference trying to ping the actor
            pass


class ProcessActor(Actor):

    # When creating an instance, the first time launch a new Python
    # process. Subsequent times (from inside the new process) just
    # create a regular instance.
    launch = True

    def __init__(self, *args, **kwargs):
        if self.launch:
            class_file = sys.modules[self.__class__.__module__].__file__
            class_dir = os.path.abspath(os.path.dirname(class_file))
            self.remote_addr, self.pid = start_actor(self.__class__.__name__,
                                                     self.__class__.__module__,
                                                     class_dir)
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

    def _act(self):
        self.receive(init=self.remote_init)
        try:
            self.act()
        except (StopIteration, ActorFinished):
            return

    @staticmethod
    def spawn(actor, name=None, ip='localhost', **kwargs):
        """
        Utility function to start a process actor and initialize it
        """
        if name is not None:
            with ActorRef((name, ip, None)) as ref:
                # Do not start if it's already alive
                if ref.is_alive():
                    return ProcessActorProxy(*ref.full_address())
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
            kwargs['ip'] = ip
            ref.init(reply_to=wait, **kwargs)
            if wait.act():
                return ProcessActorProxy(a.remote_addr, a.pid)
            else:
                raise SpawnTimeoutError('failed to init remote process')


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

def start_actor(name, module, class_dir):
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
        p = subprocess.Popen(['python', myself, w_name, name, module, class_dir])
        return w.act()

class PEcho(ProcessActor):

    def __init__(self):
        super(PEcho, self).__init__()

    def act(self):
        while True:
            self.receive(
                _ = self.do_pecho
            )

    def do_pecho(self, msg):
        print('Process Echo:')
        print(msg)


if __name__ == '__main__':
    _, wait_name, actor_class, actor_module, class_dir = sys.argv
    sys.path.insert(0, class_dir)
    mod = importlib.import_module(actor_module)
    cls = getattr(mod, actor_class)
    # Signal the base class ``ProcessActor`` to not start a new
    # subprocess (we are already in it!)
    cls.launch = False
    with cls() as actor, ActorRef(wait_name, remote=False) as wait:
        # Tell parent to keep going
        wait.ok(spawn_address=actor.address(), pid=os.getpid())

        # The new process ends when the client's actor finishes its
        # ``act`` method.
        try:
            actor._act()
        except KeyboardInterrupt:
            pass
