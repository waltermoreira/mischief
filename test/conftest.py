from pycommon.actors.actor import ThreadedActor, ProcessActor, ActorManager
import multiprocessing
import signal
import os

def teardown(x):
    x.get_named('t').put({'tag': 'stop'})
    for proc in multiprocessing.active_children():
        os.kill(proc.pid, signal.SIGKILL)
    x.shutdown()

def pytest_funcarg__qm(request):
    return request.cached_setup(
        setup=create_queue_manager,
        teardown=teardown,
        scope='session')

class _threaded_actor(ThreadedActor):

    def reply(self, msg):
        sender = msg['reply_to']
        self.send(sender, {'tag': 'answer',
                           'answer': 5})

    def queue(self, msg):
        sender = msg['reply_to']
        self.send(sender, {'tag': 'answer',
                           'answer': self.inbox.qsize()})

    def act(self):
        q = []
        while not q:
            self.receive({
                'reply': 'reply',
                'queue': 'queue',
                'stop': lambda msg: q.append(1)})

class _child(ProcessActor):

    def act(self):
        self.receive({
            'foo': 'foo'
            })

    def foo(self, msg):
        self.get_actor_ref(msg['reply_to']).send({
            'tag': 'reply',
            'data': 3
            })
        
class _process_actor(ProcessActor):

    def reply(self, msg):
        sender = msg['reply_to']
        self.send(sender, {'tag': 'answer',
                           'answer': 5})

    def queue(self, msg):
        sender = msg['reply_to']
        self.send(sender, {'tag': 'answer',
                           'answer': self.inbox.qsize()})

    def spawn(self, msg):
        _child('child')
        self.get_actor_ref(msg['reply_to']).send({
            'tag': 'reply'
            })
        
    def act(self):
        while True:
            self.receive({
                'reply': 'reply',
                'queue': 'queue',
                'spawn': 'spawn'})

def create_queue_manager():
    qm = ActorManager(address=('localhost', 5000), authkey='actor')
    qm.start()
    t = _threaded_actor('t')
    p = _process_actor('p')
    return qm

