import time

from mischief.actors.actor import ActorRef, spawn, ThreadedActor
from mischief.actors.process_actor import ProcessActor

ActorKind = ProcessActor

class Ping(ActorKind):

    def act(self):
        while True:
            if self.c >= 1000:
                self.receive(start_time=self.get_start_time)
            else:
                self.receive(ping=self.ping,
                             set_time=self.set_time)

    def get_start_time(self, msg):
        print('Ping starting time:', self.starting_time)
            
    def set_time(self, msg):
        self.starting_time = time.time()

    def ping(self, msg):
        print('ping', self.c)
        self.c += 1
        with ActorRef(msg.reply_to) as sender:
            sender.pong(reply_to=self)

class Pong(ActorKind):

    def act(self):
        while True:
            if self.c >= 999:
                print('Pong end time:', time.time())
            self.receive(pong=self.pong)

    def pong(self, msg):
        print('pong', self.c)
        self.c += 1
        with ActorRef(msg.reply_to) as sender:
            sender.ping(reply_to=self)


def run():
    ping = spawn(Ping, c=0)
    pong = spawn(Pong, c=0)
    print('Starting')
    with ActorRef(ping) as ping_ref:
        ping_ref.set_time()
        ping_ref.ping(reply_to=pong)
    return ping, pong