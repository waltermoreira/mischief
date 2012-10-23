from het2_common.actors.process_actor import ProcessActor
from het2_common.actors.actor import ActorRef
import os
import signal

class Register(ProcessActor):
    """
    An actor to register process we want to kill before quitting.
    """
    
    def __init__(self):
        super(Register, self).__init__('Register')
        self.processes = {}

    def process_act(self):
        self.receive({'register': self.register,
                      'unregister': self.unregister,
                      'killall': self.killall,
                      'list': self.list})
        
    def list(self, msg):
        for pid, name in self.processes.items():
            print '%5d: %s' %(pid, name)
            
    def register(self, msg):
        pid = msg['pid']
        name = msg['name']
        self.processes[pid] = name

    def _unregister(self, pid):
        try:
            del self.processes[pid]
        except KeyError:
            pass
        
    def unregister(self, msg):
        pid = msg['pid']
        self._unregister(pid)
        
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
            self._unregister(pid)

