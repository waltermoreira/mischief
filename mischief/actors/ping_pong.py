from het2_common.actors.actor import ThreadedActor, ActorRef
from het2_common.actors.process_actor import ProcessActor, spawn
import time

class Ping(ProcessActor):

    def __init__(self):
        self.c = 0
        super(Ping, self).__init__('ping')

    def process_act(self):
        if self.c >= 1000:
            self.receive(start_time=self.get_start_time)
        else:
            self.receive(ping=self.ping,
                         set_time=self.set_time)

    def get_start_time(self, msg):
        print self.starting_time
            
    def set_time(self, msg):
        self.starting_time = time.time()

    def ping(self, msg):
        print 'ping', self.c
        self.c += 1
        with ActorRef(msg['reply_to']) as sender:
            sender.pong(reply_to=self.name)

class Pong(ProcessActor):

    def __init__(self):
        self.c = 0
        super(Pong, self).__init__('pong')

    def process_act(self):
        if self.c >= 999:
            print time.time()
        self.receive(pong=self.pong)

    def get_end_time(self, msg):
        print time.time()
        
    def pong(self, msg):
        print 'pong', self.c
        self.c += 1
        with ActorRef(msg['reply_to']) as sender:
            sender.ping(reply_to=self.name)


def run():
    ping, _ = spawn(Ping, 'ping', c=0)
    pong, _ = spawn(Pong, 'pong', c=0)
    print 'Starting'
    with ActorRef('ping') as ping_ref:
        ping_ref.set_time()
        ping_ref.ping(reply_to='pong')
    return ping, pong