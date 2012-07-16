from pycommon.actors.actor import Actor, ActorRef, ActorException
import py

def test_non_existent_actor(qm):
    # make sure there is no actor foobar
    a = Actor('foobar')
    a.destroy_actor()
    # test it twice, because of bug in __del__ was causing second time
    # to hang up
    with py.test.raises(ActorException):
        ActorRef('foobar')
    with py.test.raises(ActorException):
        ActorRef('foobar')
        