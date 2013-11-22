import time
import os


import pytest

from mischief.actors.actor import Actor, ActorRef, ThreadedActor
from mischief.exceptions import ActorFinished, PipeException
from mischief.actors.process_actor import ProcessActor

@pytest.yield_fixture(scope='module')
def threaded_actor():
    """A threaded actor"""
    
    class _Actor(ThreadedActor):
        def reply5(self, msg):
            with ActorRef(msg['reply_to']) as sender:
                sender.answer(value=5)
        def act(self):
            try:
                while True:
                    self.receive(
                        reply5 = self.reply5,
                    )
            except ActorFinished:
                pass
                
    with _Actor() as actor:
        yield actor
        
@pytest.yield_fixture(scope='module')
def answer_actor():
    """Listen for 'answer' and return 'value'"""
    
    class _Actor(Actor):
        def act(self):
            result = []
            self.receive(
                answer = lambda msg: result.append(msg['value']))
            return result

    with _Actor() as actor:
        yield actor

@pytest.yield_fixture(scope='function')
def data_actor():
    """Actor accepts message 'foo' with field 'data'"""

    class _Actor(Actor):
        def act(self):
            self.receive(
                foo = self.read_value('data'),
                timeout=0)
            return getattr(self, 'data', None)
    with _Actor() as actor:
        yield actor

class EchoProcessActor(ProcessActor):
    def act(self):
        while True:
            self.receive(
                echo = self.echo
            )
    def echo(self, msg):
        with ActorRef(msg['reply_to']) as sender:
            del msg['tag']
            sender.reply(**msg)
        
@pytest.yield_fixture(scope='module')
def process_actor():
    """A process actor"""

    with ProcessActor.spawn(EchoProcessActor) as proxy:
        yield proxy
        
def test_reply(namebroker, threaded_actor, answer_actor):
    with ActorRef(threaded_actor.address()) as t:
        t.reply5(reply_to=answer_actor)
        result = answer_actor.act()
        assert result == [5]

def test_inbox(namebroker, threaded_actor, answer_actor):
    with ActorRef(threaded_actor.address()) as t:
        t.foo()
        t.bar()
        t.reply5(reply_to=answer_actor)
        result = answer_actor.act()
        assert result == [5]

def test_wildcard(namebroker):
    class A(Actor):
        def act(self):
            result = []
            self.receive(
                _ = lambda msg: result.append(msg['tag']))
            return result
    with A() as a, ActorRef(a.address()) as a_ref:
        a_ref.foo()
        result = a.act()
        assert result == ['foo']

def test_timeout(namebroker):
    class A(Actor):
        def act(self):
            result = [False]
            self.receive(
                timed_out = lambda msg: result.append(True),
                timeout = 0.1)
            return result
    with A() as a:
        result = a.act()
        assert result[-1]

def test_new_api(namebroker):
    class A(Actor):
        def act(self):
            self.receive(
                foo = self.foo)
            return self._msg
        def foo(self, msg):
            self._msg = msg
    with A() as a, ActorRef(a.address()) as a_ref:
        a_ref.foo(bar=3, baz='baz')
        msg = a.act()
        assert (msg['tag'] == 'foo' and
                msg['bar'] == 3 and
                msg['baz'] == 'baz')

def test_new_api_2(namebroker):
    class A(Actor):
        pass
    with A() as a, ActorRef(a.address()) as a_ref:
        with pytest.raises(TypeError):
            a_ref()
        a_ref.foo()
        with pytest.raises(TypeError):
            a_ref()

def test_many_msgs(namebroker):
    # Send many msgs to one actor, collect them and count them
    class A(ThreadedActor):
        def act(self):
            self.results = {}
            try:
                while True:
                    self.receive(add=self.add)
            except ActorFinished:
                pass
        def add(self, msg):
            self.results[msg['i']] = 1
            if list(sorted(self.results.keys())) == list(range(100)):
                with ActorRef(msg['reply_to']) as sender:
                    sender.got_all()
    class B(Actor):
        def act(self):
            result = []
            self.receive(got_all=lambda msg: result.append(True),
                         timed_out=lambda msg: result.append(False),
                         timeout=2)
            return result
    # Create many instances to check the pipes are refreshed for each instance
    actors = [A() for i in range(4)]
    collector = B()
    x_ref = ActorRef(actors[-1].address())
    for i in range(100):
        x_ref.add(i=i, reply_to=collector)
    result = collector.act()
    assert result == [True]
    [x.close() for x in actors]
    collector.close()

def test_non_existent_actor_ref():
    with pytest.raises(PipeException):
        with ActorRef('foobar') as ref:
            pass

def test_existent_actor_ref(threaded_actor):
    with ActorRef(threaded_actor.address()) as ref:
        alive = ref.is_alive()
        assert alive
    # y = ActorRef('p')
    # alive = y.is_alive()
    # assert alive
    # x.close()
    # y.close()

def test_timeout_zero(data_actor):
    with ActorRef(data_actor.address()) as a_ref:
        a_ref.foo(data=1)
        while True:
            result = data_actor.act()
            if result is not None:
                assert result == 1
                return
            time.sleep(0.1)

def test_timeout_zero_2(data_actor):
    with ActorRef(data_actor.address()) as a_ref:
        a_ref.bar()
        a_ref.baz()
        a_ref.foo(data=1)
        a_ref.gii()
        while data_actor.inbox.qsize() < 4:
            time.sleep(0.1)
        result = data_actor.act()
        assert result == 1
        
def test_timeout_zero_no_match(data_actor):
    with ActorRef(data_actor.address()) as a_ref:
        a_ref.bar(data=2)
        while data_actor.inbox.qsize() != 1:
            time.sleep(0.1)
        for _ in range(4):
            result = data_actor.act()
            assert result is None

def test_timeout_eating_msgs():
    result = [True]
    class A(Actor):
        def act(self):
            self.receive(timeout=0.1)
        def act2(self):
            self.receive(
                bar = lambda msg: None,
                timed_out = lambda msg: result.append(False),
                timeout = 0.1)
    with A() as a, ActorRef(a.address()) as a_ref:
        a_ref.bar()
        while a.inbox.qsize() != 1:
            time.sleep(0.1)
        a.act()
        a.act2()
        assert result[-1]

# # def test_process_actor_returns_name(q):
# #     p, _ = spawn(q, 'foo')
# #     ref = ActorRef('foo')
# #     assert p == 'foo'
# #     ActorRef('foo').close_actor()
    
# # def test_process_with_arg(q2):
# #     class a(Actor):
# #         def act(self):
# #             self.receive(reply = self.read_value('x'))
# #             return self.x
# #     x = a()
# #     spawn(q2, 'foo2', x=5)
# #     ref = ActorRef('foo2')
# #     ref.get_x(reply_to=x.name)
# #     u = x.act()
# #     assert u == 5
# #     ref.close_actor()
# #     x.close()

def test_close_with_confirmation(threaded_actor):
    class A(Actor):
        def act(self):
            self.receive(_ = self.read_value('tag'))
            return self.tag
    class T(ThreadedActor):
        def act(self):
            self.receive()
    with A() as a, T() as t, ActorRef(t.address()) as t_ref:
        alive = t_ref.is_alive()
        assert alive
        t_ref.close_actor(confirm_to=a.address())
        result = a.act()
        assert result == 'closed'
        assert not t_ref.is_alive()
        
def test_ping(threaded_actor):
    with ActorRef(threaded_actor.address()) as t_ref:
        assert t_ref.is_alive()
        time.sleep(0.5)
        assert t_ref.is_alive()

def test_none_method():
    class A(Actor):
        def act(self):
            self.receive(foo=None)
            return True
    with A() as a, ActorRef(a.address()) as a_ref:
        a_ref.foo()
        result = a.act()
        assert result

def test_close_actor_and_ref():
    class Wait(Actor):
        def act(self):
            self.receive(_=None)
    class T(ThreadedActor):
        def act(self):
            self.receive()
    with T() as t, Wait() as w, ActorRef(t.address()) as t_ref:
        alive = t_ref.is_alive()
        assert alive
        t_ref.close_actor(confirm_to=w.address())
        w.act()
        not_alive = not t_ref.is_alive()
        assert not_alive

def test_sync_call():
    class T(ThreadedActor):
        def act(self):
            self.receive(sync_test=self.sync_test)
        def sync_test(self, msg):
            with ActorRef(msg['reply_to']) as sender:
                sender.reply(got=msg)
    with T() as t, ActorRef(t.address()) as t_ref:
        assert t_ref.is_alive()
        answer = t_ref.sync('sync_test', x=5)
        assert answer['got']['tag'] == 'sync_test'
        assert answer['got']['x'] == 5

def test_alive_not_acting():
    class A(Actor):
        def act(self):
            self.receive(_=self.read_value('tag'))
            return self.tag
    with A() as a, ActorRef(a.address()) as a_ref:
        alive = a_ref.is_alive()
        assert alive
        a_ref.foo()
        result = a.act()
        assert result == 'foo'

def test_threaded_spawn():
    class T(ThreadedActor):
        def act(self):
            self.receive()
    with ThreadedActor.spawn(T) as t, ActorRef(t.address()) as t_ref:
        assert t_ref.is_alive()

def test_threaded_spawn_with_args():
    class T(ThreadedActor):
        def __init__(self, name=None, ip='localhost', x=0, k=None):
            super().__init__(name, ip)
            self.x = x
            self.k = k
        def act(self):
            self.receive(
                args = self.args
            )
        def args(self, msg):
            with ActorRef(msg['reply_to']) as sender:
                sender.reply(x=self.x, k=self.k)
    class A(Actor):
        def act(self):
            self.receive(
                reply = self.reply
            )
            return self.x, self.k
        def reply(self, msg):
            self.x = msg['x']
            self.k = msg['k']
    with ThreadedActor.spawn(T, x=5, k='a') as t, ActorRef(t.address()) as t_ref, A() as a:
        assert t_ref.is_alive()
        t_ref.args(reply_to=a.address())
        result = a.act()
        assert result == (5, 'a')

def test_process_spawn(process_actor):
    with ActorRef(process_actor.address()) as p_ref:
        assert p_ref.is_alive()
        result = p_ref.sync('echo', x=4)
        assert result['x'] == 4

def test_process_close():
    with ProcessActor.spawn(EchoProcessActor) as p, ActorRef(p.address()) as p_ref:
        assert p_ref.is_alive()
        p_ref.close_actor()
        assert not p_ref.is_alive()
        for _ in range(200):
            try:
                if os.waitpid(p.pid, os.WNOHANG) == (0, 0):
                    break
            except ChildProcessError:
                break
            time.sleep(0.01)
        else:
            assert False
            