from het2_common.actors.process_actor import ProcessActor

class Foo(ProcessActor):

    def __init__(self):
        super(Foo, self).__init__('FooProcActor')

    def act(self):
        print 'Foo is going to act'
        self.receive({'hi': self.hi})

    def hi(self, msg):
        print 'Hi!!'