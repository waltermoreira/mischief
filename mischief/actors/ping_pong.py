import time

from mischief.actors.actor import Actor, ActorRef, spawn, ThreadedActor, run_forever
from mischief.actors.process_actor import ProcessActor

ActorKind = ProcessActor

class Ticker(ActorKind):

    def act(self):
        while True:
            self.receive(
                set_start_time=self.set_start_time,
                tick=self.tick,
            )

    def set_start_time(self, msg):
        self.starting_time = time.time()

    def tick(self, msg):
        print('tick', self.my_name, self.c)
        self.c += 1
        with ActorRef(msg.reply_to) as sender:
            sender.tick(reply_to=self)
        if self.c == self.max:
            with ActorRef(self.report_to) as ref:
                ref.report(
                    name=self.my_name,
                    start_time=self.starting_time,
                    end_time=time.time()
                )
            self.receive()

class Collector(ActorKind):

    def act(self):
        self.receive(report=self.first)

    def show(self, msg):
        print(msg.name)
        print('Total duration:', msg.end_time - msg.start_time, 'seconds')
        
    def first(self, msg):
        print('--- First Report ---')
        self.show(msg)
        self.receive(report=self.second)

    def second(self, msg):
        print('--- Second Report ---')
        self.show(msg)
        self.receive()
        
    
def run():
    with spawn(Collector) as collector, \
         spawn(Ticker, my_name='Ping', c=0, max=10, report_to=collector.address()) as ping, \
         spawn(Ticker, my_name='Pong', c=0, max=10, report_to=collector.address()) as pong, \
         ActorRef(ping) as ping_ref, \
         ActorRef(pong) as pong_ref:
        print('Starting')
        ping_ref.set_start_time()
        pong_ref.set_start_time()
        ping_ref.tick(reply_to=pong_ref)
        run_forever()