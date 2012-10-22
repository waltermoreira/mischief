from het2_common.actors.actor import Actor, ActorRef, ThreadedActor
import py

def test_reply(t):
    class a(Actor):
        def act(self):
            result = []
            self.receive(
                answer = lambda msg: result.append(msg['answer']))
            return result[0]
    x = a()
    qt = ActorRef('t')
    qt.reply(reply_to=x.name)
    assert x.act() == 5
    x.close()

def test_inbox(t):
    class a(Actor):
        def act(self):
            result = []
            self.receive({
                'answer': lambda msg: result.append(msg['answer'])})
            return result[0]
    x = a()
    qt = ActorRef('t')
    qt.foo()
    qt.bar()
    qt.queue(reply_to=x.name)
    assert x.act() == 2
    x.close()

def test_wildcard(t):
    class a(Actor):
        def act(self):
            result = []
            self.receive(
                _ = lambda msg: result.append(msg['tag']))
            return result
    x = a()
    qa = ActorRef(x.name)
    qa.foo()
    assert x.act() == ['foo']
    x.close()

def test_timeout(t):
    class a(Actor):
        def act(self):
            result = [False]
            self.receive(
                timed_out = lambda msg: result.append(True),
                timeout = 0.1)
            return result
    x = a()
    assert x.act()[-1]
    x.close()

def test_read_value(t):
    class a(Actor):
        def act(self):
            self.receive(
                ack = self.read_value('foo'))
            return self.foo
    x = a()
    qm = ActorRef(x.name)
    qm.ack(foo=5)
    assert x.act() == 5
    x.close()


def test_new_api(t):
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
    x.close()

def test_new_api_2(t):
    class a(Actor):
        pass
    x = a()
    qx = ActorRef(x.name)
    with py.test.raises(TypeError):
        qx()
    qx.foo()
    with py.test.raises(TypeError):
        qx()
    x.close()

def test_many_msgs(t):
    # Send many msgs to one actor, collect them and count them
    class a(ThreadedActor):
        def __init__(self):
            super(a, self).__init__()
            self.results = {}
        def act(self):
            while True:
                self.receive(add=self.add)
        def add(self, msg):
            self.results[msg['i']] = 1
            if sorted(self.results.keys()) == range(100):
                with ActorRef(msg['reply_to']) as sender:
                    sender.got_all()
    class b(Actor):
        def act(self):
            result = []
            self.receive(got_all=lambda msg: result.append(True),
                         timed_out=lambda msg: result.append(False),
                         timeout=2)
            return result
    # Create many instances to check the pipes are refreshed for each instance
    actors = [a() for i in range(4)]
    y = b()
    x_ref = ActorRef(actors[-1].name)
    for i in range(100):
        x_ref.add(i=i, reply_to=y.name)
    res = y.act()
    assert res == [True]
    [x.close() for x in actors]
    y.close()
    