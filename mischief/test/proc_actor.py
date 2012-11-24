from het2_common.actors.actor import ActorRef
from het2_common.actors.process_actor import ProcessActor
import subprocess
import os.path
import sys
import time

class _process_actor(ProcessActor):

    def __init__(self):
        super(_process_actor, self).__init__('p')
        
    def reply(self, msg):
        with ActorRef(msg['reply_to']) as sender:
            sender.answer(answer=5)

    def queue(self, msg):
        with ActorRef(msg['reply_to']) as sender:
            sender.answer(answer=2)

    def quit(self, msg):
        sys.exit(0)
        
    def process_act(self):
        self.receive(
            reply = self.reply,
            queue = self.queue,
            quit = self.quit)

class _with_name(ProcessActor):

    def __init__(self):
        super(_with_name, self).__init__('foo')
        self.x = None
        
    def process_act(self):
        self.receive(get_x=self.get_x)

    def get_x(self, msg):
        with ActorRef(msg['reply_to']) as sender:
            sender.reply(x=self.x)

class _with_name_2(_with_name):

    def __init__(self):
        ProcessActor.__init__(self, 'foo2')
        self.x = None
