import sys
import os

import pytest

# Append directory where the current package
sys.path.insert(
    0, os.path.join('../../', os.path.dirname(os.path.abspath(__file__))))

import mischief.actors.pipe as p
import mischief.actors.namebroker as n

@pytest.yield_fixture(scope='session')
def namebroker():
    """Start and return a client for the namebroker."""
    client = n.NameBrokerClient()
    nb = None
    if not client.is_server_alive():
        nb = n.NameBroker()
        nb.start()
    yield client
    if nb is not None:
        nb.stop()
    

# from mischief.actors.actor import Actor, ActorRef, ThreadedActor
# from mischief.actors.pipe import NameBroker

# # from mischief.actors.actor import ThreadedActor, ActorRef, Actor
# # #from mischief.actors.process_actor import spawn
# # import multiprocessing
# # import signal
# # import time
# # import os
# # from proc_actor import _process_actor, _with_name, _with_name_2

# # # def destroy():
# # #     ActorRef('t').destroy_actor()
# # #     ActorRef('p').send({'tag': 'quit'})
# # #     ActorRef('p').destroy_actor()
    
# def pytest_funcarg__p(request):
#     return request.cached_setup(
#         setup=create_process_actor,
#         teardown=lambda x: x.close_actor(),
#         scope='session')

# def create_process_actor():
#     spawn(_process_actor, name='p')
#     ref = ActorRef('p')
#     for _ in range(100):
#         if ref.is_alive():
#             break
#         time.sleep(0.1)
#     return ref

# # def pytest_funcarg__q(request):
# #     return _with_name

# # def pytest_funcarg__q2(request):
# #     return _with_name_2