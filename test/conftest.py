from pycommon.actors.actor import ThreadedActor, ProcessActor, ActorManager

def pytest_funcarg__qm(request):
    return request.cached_setup(
        setup=create_queue_manager,
        teardown=lambda x: x.get_named('t').put({'tag': 'stop'}),
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

class _process_actor(ProcessActor):

    def reply(self, msg):
        sender = msg['reply_to']
        self.send(sender, {'tag': 'answer',
                           'answer': 5})

    def queue(self, msg):
        sender = msg['reply_to']
        self.send(sender, {'tag': 'answer',
                           'answer': self.inbox.qsize()})

    def act(self):
        while True:
            self.receive({
                'reply': 'reply',
                'queue': 'queue'})

def create_queue_manager():
    qm = ActorManager(address=('localhost', 5000), authkey='actor')
    qm.start()
    t = _threaded_actor('t')
    p = _process_actor('p')
    return qm

