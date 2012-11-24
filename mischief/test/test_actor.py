from het2_common.actors.actor import Actor, ActorRef, ThreadedActor, ActorFinished
from het2_common.actors.process_actor import spawn
from het2_common.globals import DEPLOY_PATH
import time
import py
import os

# Add path of tests to PYTHONPATH, since ProcessActor need to be in
# the path
os.environ['PYTHONPATH'] = ':'.join([os.environ['PYTHONPATH'],
                                     os.path.join(DEPLOY_PATH, 'test')])

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
            try:
                while True:
                    self.receive(add=self.add)
            except ActorFinished:
                pass
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

def test_reply(p):
    class a(Actor):
        def act(self):
            result = []
            self.receive({
                'answer': lambda msg: result.append(msg['answer'])})
            return result[0]
    x = a('a')
    qt = ActorRef('p')
    qt.send({'tag': 'reply',
             'reply_to': 'a'})
    assert x.act() == 5
    x.close()
    

def test_inbox(p):
    class a(Actor):
        def act(self):
            result = []
            self.receive({
                'answer': lambda msg: result.append(msg['answer'])})
            return result[0]
    x = a('a')
    qt = ActorRef('p')
    qt.send({'tag': 'foo'})
    qt.send({'tag': 'bar'})
    qt.send({'tag': 'queue',
            'reply_to': 'a'})
    assert x.act() == 2
    x.close()

def test_non_existent_actor_ref():
    x = ActorRef('foobar')
    not_alive = not x.is_alive()
    assert not_alive
    not_alive = not x.is_alive()
    assert not_alive
    x.close()

def test_existent_actor_ref(t, p):
    x = ActorRef('t')
    alive = x.is_alive()
    assert alive
    y = ActorRef('p')
    alive = y.is_alive()
    assert alive
    x.close()
    y.close()

def test_timeout_zero():
    class a(Actor):
        def act(self):
            self.receive({
                'foo': self.read_value('data'),
                }, timeout=0)
            return getattr(self, 'data', None)
    x = a()
    ActorRef(x.name).send({'tag': 'foo', 'data': 1})
    while x.act() is None:
        time.sleep(0.1)
    y = x.act()
    assert y == 1
    x.close()

def test_timeout_zero_2():
    class a(Actor):
        def act(self):
            self.receive({
                'foo': self.read_value('data'),
                }, timeout=0)
            return getattr(self, 'data', None)
    x = a()
    y = ActorRef(x.name)
    y.send({'tag': 'bar'})
    y.send({'tag': 'baz'})
    y.send({'tag': 'foo', 'data': 1})
    y.send({'tag': 'gii'})
    while x.inbox.qsize() < 4:
        time.sleep(0.1)
    z = x.act()
    assert z == 1
    x.close()
        
def test_timeout_zero_no_match():
    class a(Actor):
        def act(self):
            self.receive({
                'foo': self.read_value('data')
                }, timeout=0)
            return getattr(self, 'data', None)
    x = a()
    ActorRef(x.name).send({'tag': 'bar', 'data': 2})
    while x.inbox.qsize() != 1:
        time.sleep(0.1)
    y = x.act()
    y = x.act()
    y = x.act()
    y = x.act()
    assert y == None
    x.close()

def test_timeout_eating_msgs():
    result = [True]
    class a(Actor):
        def act(self):
            self.receive({}, timeout=0.1)
        def act2(self):
            self.receive(
                bar = lambda msg: None,
                timed_out = lambda msg: result.append(False),
                timeout = 0.1)
    x = a()
    ActorRef(x.name).send({'tag': 'bar'})
    while x.inbox.qsize() != 1:
        time.sleep(0.1)
    x.act()
    x.act2()
    assert result[-1]
    x.close()

def test_process_actor_returns_name(q):
    p, _ = spawn(q, 'foo')
    ref = ActorRef('foo')
    assert p == 'foo'
    ActorRef('foo').close_actor()
    
def test_process_with_arg(q2):
    class a(Actor):
        def act(self):
            self.receive(reply = self.read_value('x'))
            return self.x
    x = a()
    spawn(q2, 'foo2', x=5)
    ref = ActorRef('foo2')
    ref.get_x(reply_to=x.name)
    u = x.act()
    assert u == 5
    ref.close_actor()
    x.close()

def test_close_with_confirmation(t):
    class a(Actor):
        def act(self):
            self.receive(_ = self.read_value('tag'))
            return self.tag
    x = a()
    with ActorRef('t') as tr:
        alive = tr.is_alive()
        assert alive
        tr.close_actor(confirm_to=x.name)
        u = x.act()
        assert u == 'closed'
    x.close()

def test_ping():
    class a(ThreadedActor):
        def act(self):
            try:
                self.receive(foo=lambda msg: None)
            except ActorFinished:
                pass
    x = a()
    xr = ActorRef(x.name)
    alive = xr.is_alive()
    assert alive
    time.sleep(0.5)
    alive = xr.is_alive()
    assert alive
    x.close()

def test_none_method():
    class a(Actor):
        def act(self):
            self.receive(foo=None)
            return True
    x = a()
    xr = ActorRef(x.name)
    xr.foo()
    y = x.act()
    assert y
    x.close()

def test_close_actor_and_ref():
    class Wait(Actor):
        def act(self):
            self.receive(_=None)
    class a(ThreadedActor):
        def act(self):
            self.receive()
    x = a()
    wait = Wait()
    xr = ActorRef(x.name)
    alive = xr.is_alive()
    assert alive
    xr.close_actor(confirm_to=wait.name)
    wait.act()
    not_alive = not xr.is_alive()
    assert not_alive
    x.close()
    wait.close()

def test_sync_call(t):
    class a(ThreadedActor):
        def act(self):
            self.receive(sync_test=self.sync_test)
        def sync_test(self, msg):
            with ActorRef(msg['reply_to']) as sender:
                sender.reply(got=msg)
    x = a()
    with ActorRef(x.name) as tr:
        assert tr.is_alive()
        answer = tr.sync('sync_test', x=5)
        assert answer['got']['tag'] == 'sync_test'
        assert answer['got']['x'] == 5
    x.close()

def test_alive_not_acting():
    class a(Actor):
        def act(self):
            self.receive(_=self.read_value('tag'))
            return self.tag
    x = a()
    xr = ActorRef(x.name)
    alive = xr.is_alive()
    assert alive
    xr.foo()
    u = x.act()
    assert u == 'foo'