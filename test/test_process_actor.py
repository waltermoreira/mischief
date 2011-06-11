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

def test_actor_spawns_actor(qm):
    class a(Actor):
        def act(self):
            qm.get_actor_ref('p').send({
                'tag': 'spawn',
                'reply_to': self.me()
                })
            self.receive({
                'reply': lambda msg: None
                })
            qm.get_actor_ref('child').send({
                'tag': 'foo',
                'reply_to': self.me()
                })
            self.receive({
                'reply': self.read_value('data')
                })
            return self.data
    x = a('a')
    assert x.act() == 3
