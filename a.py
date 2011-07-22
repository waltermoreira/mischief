import pycommon.actors.actor as actor

import logging
import logging.handlers

log = logging.getLogger('test')
handler = logging.handlers.RotatingFileHandler('/tmp/test.log')
formatter = logging.Formatter('%(asctime)s %(levelname)-8s [%(filename)30s:%(lineno)-4s] - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.DEBUG)

class Bar(actor.ProcessActor):

    def act(self):
        self.receive({'bar': 'bar'})

    def bar(self, msg):
        print '>>>> Bar'
        
class Foo(actor.ProcessActor):

    def __init__(self):
        super(Foo, self).__init__()
        self.bar = Bar()

    def act(self):
        self.receive({'foo': 'foo',
                      'stop': 'stop'})

    def foo(self, msg):
        print '>>>> Foo'

    def stop(self, msg):
        pass

am = actor.ActorManager()
am.start()

actors = []
for i in range(100):
    log.debug('Creating Foo %s' %i)
    x = Foo()
    log.debug('Created Foo %s' %i)
    actors.append(x)
    x.get_actor_ref(x.name).send({'tag': 'foo'})
    x.get_actor_ref(x.bar.name).send({'tag': 'bar'})

# for ac in actors:
#     ac.get_actor_ref(ac.name).send({'tag': 'foo'})
    
