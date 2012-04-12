from pycommon.actors.process_actor import ProcessActor
from pycommon.actors.actor import ActorRef
import os
import signal

class Register(ProcessActor):
    """
    An actor to register process we want to kill before quitting.
    """
    
    def __init__(self):
        super(Register, self).__init__('Register')
        self.processes = {}

    def act(self):
        try:
            while True:
                self.receive({'register': self.register,
                              'unregister': self.unregister,
                              'killall': self.killall,
                              'quit': self.quit})
        except StopIteration:
            return

    def register(self, msg):
        pid = msg['pid']
        name = msg['name']
        self.processes[pid] = name

    def unregister(self, msg):
        pid = msg['pid']
        try:
            del self.processes[pid]
        except KeyError:
            pass

    def killall(self, msg):
        for pid, name in self.processes.items():
            try:
                ActorRef(name).destroy_actor()
            except OSError:
                pass
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass

    def quit(self, msg):
        raise StopIteration