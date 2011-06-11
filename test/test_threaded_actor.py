from pycommon.actors.actor import Actor
import uuid

def test_reply(qm):
    class a(Actor):
        def act(self):
            result = []
            self.receive({
                'answer': lambda msg: result.append(msg['answer'])})
            return result[0]
    x = a('a')
    qt = qm.get_named('t')
    qt.put({'tag': 'reply',
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
    qt = qm.get_named('t')
    qt.put({'tag': 'foo'})
    qt.put({'tag': 'bar'})
    qt.put({'tag': 'queue',
            'reply_to': 'a'})
    assert x.act() == 2

def test_wildcard(qm):
    class a(Actor):
        def act(self):
            result = []
            self.receive({
                '*': lambda msg: result.append(msg['tag'])})
            return result
    x = a('a')
    qa = qm.get_named('a')
    qa.put({'tag': 'foo'})
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
    x = a('a')
    assert x.act()[-1]

def test_read_value(qm):
    class a(Actor):
        def act(self):
            self.receive({
                'ack': self.read_value('foo')
                })
            return self.foo
    x = a('a')
    qm.get_actor_ref('a').send({'tag': 'ack', 'foo': 5})
    assert x.act() == 5

def test_unnamed(qm):
    class a(Actor):
        def act(self):
            return self.me()
    x = a()
    assert x.me()[-17:] == str(uuid.uuid1())[-17:]
        
