from pycommon.actors.actor import Actor, ActorRef
import time

def test_reply(qm):
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
    
def test_inbox(qm):
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

def test_timeout_zero(qm):
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

def test_timeout_zero_2(qm):
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
        
def test_timeout_zero_no_match(qm):
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

def test_timeout_eating_msgs(qm):
    result = [True]
    class a(Actor):
        def act(self):
            self.receive({}, timeout=0.1)
        def act2(self):
            self.receive({'bar': lambda msg: None,
                          'timeout': lambda msg: result.append(False)},
                         timeout=0.1)
    x = a()
    ActorRef(x.name).send({'tag': 'bar'})
    while x.inbox.qsize() != 1:
        time.sleep(0.1)
    x.act()
    x.act2()
    assert result[-1]

