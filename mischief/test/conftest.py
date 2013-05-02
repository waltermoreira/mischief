import sys
import os

# Append directory where the current package
sys.path.append(
    os.path.join('../../', os.path.dirname(os.path.abspath(__file__))))

from mischief.actors.actor import Actor, ActorRef, ThreadedActor
from mischief.actors.pipe import NameBroker

# from mischief.actors.actor import ThreadedActor, ActorRef, Actor
# #from mischief.actors.process_actor import spawn
# import multiprocessing
# import signal
# import time
# import os
# from proc_actor import _process_actor, _with_name, _with_name_2

# # def destroy():
# #     ActorRef('t').destroy_actor()
# #     ActorRef('p').send({'tag': 'quit'})
# #     ActorRef('p').destroy_actor()

class _threaded_actor(ThreadedActor):

    def reply5(self, msg):
        sender = ActorRef(msg['reply_to'])
        sender.answer(answer=5)

    def reply2(self, msg):
        sender = ActorRef(msg['reply_to'])
        sender.answer(answer=2)

    def act(self):
        while True:
            self.receive(
                reply5 = self.reply5,
                reply2 = self.reply2)

def pytest_funcarg__t(request):
    return request.cached_setup(
        setup=lambda: _threaded_actor(name='t'),
        teardown=lambda x: x.close(),
        scope='session')

def start_namebroker():
    x = NameBroker()
    x.start()
    return x
    
def pytest_funcarg__nb(request):
    return request.cached_setup(
        setup=start_namebroker,
        teardown=lambda x: x.stop(),
        scope='session')
    
def pytest_funcarg__p(request):
    return request.cached_setup(
        setup=create_process_actor,
        teardown=lambda x: x.close_actor(),
        scope='session')

def create_process_actor():
    spawn(_process_actor, name='p')
    ref = ActorRef('p')
    for _ in range(100):
        if ref.is_alive():
            break
        time.sleep(0.1)
    return ref

# def pytest_funcarg__q(request):
#     return _with_name

# def pytest_funcarg__q2(request):
#     return _with_name_2