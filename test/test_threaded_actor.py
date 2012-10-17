from het2_common.actors.actor import Actor, ActorRef, ThreadedActor
import uuid
import py

def test_reply(qm):
    class a(Actor):
        def act(self):
            result = []
            self.receive({
                'answer': lambda msg: result.append(msg['answer'])})
            return result[0]
    x = a()
    qt = ActorRef('t')
    qt.send({'tag': 'reply',
             'reply_to': x.name})
    assert x.act() == 5
    
def test_inbox(qm):
    class a(Actor):
        def act(self):
            result = []
            self.receive({
                'answer': lambda msg: result.append(msg['answer'])})
            return result[0]
    x = a()
    qt = ActorRef('t')
    qt.send({'tag': 'foo'})
    qt.send({'tag': 'bar'})
    qt.send({'tag': 'queue',
             'reply_to': x.name})
    assert x.act() == 2

def test_wildcard(qm):
    class a(Actor):
        def act(self):
            result = []
            self.receive({
                '*': lambda msg: result.append(msg['tag'])})
            return result
    x = a()
    qa = ActorRef(x.name)
    qa.send({'tag': 'foo'})
    assert x.act() == ['foo']

def test_timeout(qm):
    class a(Actor):
        def act(self):
            result = [False]
            self.receive(
                {
                'timeout': lambda msg: result.append(True)},
                timeout=0.1)
            return result
    x = a()
    assert x.act()[-1]

def test_read_value(qm):
    class a(Actor):
        def act(self):
            self.receive({
                'ack': self.read_value('foo')
                })
            return self.foo
    x = a()
    qm = ActorRef(x.name)
    qm.send({'tag': 'ack', 'foo': 5})
    assert x.act() == 5

def test_new_api(qm):
    class a(Actor):
        def act(self):
            self.receive({'foo': self.foo})
            return self._msg
        def foo(self, msg):
            self._msg = msg
    x = a()
    qx = ActorRef(x.name)
    qx.foo(bar=5, baz='baz')
    msg = x.act()
    assert (msg['tag'] == 'foo' and
            msg['bar'] == 5 and
            msg['baz'] == 'baz')

def test_new_api_2(qm):
    class a(Actor):
        pass
    x = a()
    qx = ActorRef(x.name)
    with py.test.raises(TypeError):
        qx()
    qx.foo()
    with py.test.raises(TypeError):
        qx()

def test_many_msgs(qm):
    # Send many msgs to one actor, collect them and count them
    class a(ThreadedActor):
        def __init__(self):
            super(a, self).__init__(name='foo')
            self.results = {}
        def act(self):
            while True:
                self.receive({'add': self.add})
        def add(self, msg):
            self.results[msg['i']] = 1
            if sorted(self.results.keys()) == range(100):
                sender = ActorRef(msg['reply_to'])
                sender.got_all()
                sender.destroy_ref()
    class b(Actor):
        def act(self):
            result = []
            self.receive({'got_all': lambda msg: result.append(True),
                          'timeout': lambda msg: result.append(False)},
                         timeout=2)
            return result
    # Create many instances to check the pipes are refreshed for each instance
    x = a()
    x = a()
    x = a()
    x = a()
    y = b()
    x_ref = ActorRef(x.me())
    for i in range(100):
        x_ref.add(i=i, reply_to=y.me())
    res = y.act()
    assert res == [True]
    