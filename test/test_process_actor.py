from pycommon.actors.actor import Actor

def test_reply(qm):
    class a(Actor):
        def act(self):
            result = []
            self.receive({
                'answer': lambda msg: result.append(msg['answer'])})
            return result[0]
    x = a('a')
    qt = qm.get_named('p')
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
    qt = qm.get_named('p')
    qt.put({'tag': 'foo'})
    qt.put({'tag': 'bar'})
    qt.put({'tag': 'queue',
            'reply_to': 'a'})
    assert x.act() == 2

