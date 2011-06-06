from pycommon.actors.actor import Actor

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
