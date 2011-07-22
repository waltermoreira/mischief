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

# def test_actor_spawns_actor(qm):
#     class a(Actor):
#         def act(self):
#             qm.get_actor_ref('p').send({
#                 'tag': 'spawn',
#                 'reply_to': self.me()
#                 })
#             self.receive({
#                 'reply': lambda msg: None
#                 })
#             qm.get_actor_ref('child').send({
#                 'tag': 'foo',
#                 'reply_to': self.me()
#                 })
#             self.receive({
#                 'reply': self.read_value('data')
#                 })
#             return self.data
#     x = a('a')
#     assert x.act() == 3

def test_timeout_zero(qm):
    class a(Actor):
        def act(self):
            self.receive({
                'foo': self.read_value('data'),
                }, timeout=0)
            return getattr(self, 'data', None)
    x = a('a')
    qm.get_actor_ref('a').send({'tag': 'foo', 'data': 1})
    assert x.act() == 1

def test_timeout_zero_no_match(qm):
    class a(Actor):
        def act(self):
            self.receive({
                'foo': self.read_value('data'),
                }, timeout=0)
            return getattr(self, 'data', None)
    x = a('a')
    qm.get_actor_ref('a').send({'tag': 'bar', 'data': 2})
    assert x.act() == None

def test_timeout_eating_msgs(qm):
    result = [True]
    class a(Actor):
        def act(self):
            self.receive({}, timeout=0.1)
        def act2(self):
            self.receive({'bar': lambda msg: None,
                          'timeout': lambda msg: result.append(False)},
                         timeout=0.1)
    x = a('a')
    qm.get_actor_ref('a').send({'tag': 'bar'})
    x.act()
    x.act2()
    assert result[-1]

