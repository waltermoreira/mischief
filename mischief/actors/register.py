import os
import signal

from .process_actor import ProcessActor
from .actor import ActorRef

class Register(ProcessActor):
    """
    An actor to register process we want to kill before quitting.
    """
    
    def __init__(self):
        super().__init__('Register')
        self.processes = {}

    def act(self):
        while True:
            self.receive(
                register = self.register,
                unregister = self.unregister,
                killall = self.killall,
                show = self.show
            )
        
    def show(self, msg):
        print('List of processes registered:')
        for pid, name in self.processes.items():
            print('{:5d}: {}'.format(pid, name))
            
    def register(self, msg):
        self.processes[msg.pid] = msg.name

    def _unregister(self, pid):
        try:
            del self.processes[pid]
        except KeyError:
            pass
        
    def unregister(self, msg):
        self._unregister(msg.pid)
        
    def killall(self, msg):
        for pid, name in self.processes.items():
            try:
                with ActorRef(name) as ref:
                    ref.destroy_actor()
            except OSError:
                pass
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
            self._unregister(pid)

