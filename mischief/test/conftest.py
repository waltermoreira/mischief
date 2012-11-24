from het2_common.actors.actor import ThreadedActor, ActorRef, Actor
from het2_common.actors.process_actor import spawn
import multiprocessing
import signal
import time
import os
from proc_actor import _process_actor, _with_name, _with_name_2

# def pytest_funcarg__qm(request):
#     return request.cached_setup(
#         setup=create_queue_manager,
#         teardown=lambda x: destroy(),
#         scope='session')

# def destroy():
#     ActorRef('t').destroy_actor()
#     ActorRef('p').send({'tag': 'quit'})
#     ActorRef('p').destroy_actor()

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


# def create_queue_manager():
#     t = _threaded_actor('t')
#     process_actor()
#     p = _process_actor('p')

def pytest_funcarg__t(request):
    return request.cached_setup(
        setup=create_threaded_actor,
        teardown=lambda x: x.close(),
        scope='session')

def create_threaded_actor():
    return _threaded_actor('t')

def pytest_funcarg__p(request):
    return request.cached_setup(
        setup=create_process_actor,
        teardown=lambda x: x.close_actor(),
        scope='session')

def create_process_actor():
    spawn(_process_actor, 'p')
    ref = ActorRef('p')
    while not ref.is_alive():
        time.sleep(0.1)
    return ref

def pytest_funcarg__q(request):
    return _with_name

def pytest_funcarg__q2(request):
    return _with_name_2