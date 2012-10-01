from het2_common.actors.actor import ThreadedActor, ActorRef
import multiprocessing
import signal
import time
import os
from proc_actor import process_actor

def pytest_funcarg__qm(request):
    return request.cached_setup(
        setup=create_queue_manager,
        teardown=lambda x: destroy(),
        scope='session')

def destroy():
    ActorRef('t').destroy_actor()
    ActorRef('p').send({'tag': 'quit'})
    ActorRef('p').destroy_actor()

class _threaded_actor(ThreadedActor):

    def reply(self, msg):
        sender = ActorRef(msg['reply_to'])
        sender.send({'tag': 'answer',
                     'answer': 5})

    def queue(self, msg):
        sender = ActorRef(msg['reply_to'])
        sender.send({'tag': 'answer',
                     'answer': 2})

    def act(self):
        q = []
        while not q:
            self.receive({
                'reply': 'reply',
                'queue': 'queue',
                'stop': lambda msg: q.append(1)})


def create_queue_manager():
    t = _threaded_actor('t')
    process_actor()
    #p = _process_actor('p')
