"""
:mod:`pycommon.actors.actor` -- Actor library
=============================================

.. module:: actor
.. moduleauthor:: Walter Moreira <moreira@astro.as.utexas.edu>

An implementation of the `actor model`_.

A queue manager must be created somewhere, and started::

  qm = ActorManager(address=('localhost', 5000), authkey='actor')
  qm.start()

An actor inherits from ``ThreadedActor`` or ``ProcessActor``, and it
implements the method ``act()``::

  class MyActor(ThreadedActor):
      def act(self):
          self.receive({
              'foo': lambda msg: None
              })

.. _actor model: http://en.wikipedia.org/wiki/Actor_model
"""

import multiprocessing as m
import multiprocessing.managers as managers
import threading
import Queue
import sys
import os
import signal
import time

class ConnectionManager(managers.BaseManager):
    pass

class ActorManager(managers.BaseManager):
    """
    An actor manager handles the creation of actors and delivers
    references on demand.

    It must run before starting the actors::

        m = ActorManager()
        m.start()

    For using it::

        m = ActorManager()
        m.connect()
        foo = m.get_actor_ref('foo')
        foo.send(msg)
    """
    
    def __init__(self, *args, **kwargs):
        kwargs['address'] = ('localhost', 5000)
        kwargs['authkey'] = 'actor'
        super(ActorManager, self).__init__(*args, **kwargs)
        self._manager = m.Manager()
        self.register('create_queue', callable=self.create_queue)
        self.register('get_named', callable=self.get_named)
        self.register('destroy_named', callable=self.destroy_named)
        self.named_queues = {}
        
    def create_queue(self, name):
        self.named_queues[name] = self._manager.Queue()

    def get_named(self, name):
        return self.named_queues[name]

    def destroy_named(self, name):
        del self.name_queues[name]

    def get_actor_ref(self, name):
        """
        Get a reference to an actor::

           x = m.get_actor_ref('foo')
           x.send(...)
           
        """
        return ActorRef(self.get_named(name), name)
    
class ActorRef(object):
    """
    An actor reference.
    """
    
    def __init__(self, q, name):
        self.q = q
        self.name = name

    def send(self, msg):
        """
        Send a message to the actor represented by this reference.
        """
        self.q.put(msg)

    def me(self):
        """
        Name of the actor pointed by this reference. Use it for
        sending self references in the messages::

            {'tag': 'reply_me',
             'sender': self.me()}
        """
        return self.name
        
class Actor(object):
    """
    Messages to the actor have the form::

      {'tag': 'foo',
       ...}
    """

    # When a timeout is given in a ``receive``, check every
    # ``INBOX_POLLING_TIMEOUT`` seconds whether we have timed out.
    INBOX_POLLING_TIMEOUT = 0.01
    
    def __init__(self, name):
        self.name = name
        self.qm = ActorManager()
        self.qm.connect()
        self.qm.create_queue(self.name)
        self.inbox = self.qm.get_named(self.name)

    def get_inbox(self, name):
        return self.qm.get_named(name)

    def send(self, to, msg):
        self.get_inbox(to).put(msg)

    def get_actor_ref(self, name):
        """
        Get a reference to actor named ``name``.
        """
        return self.qm.get_actor_ref(name)
    
    def receive(self, patterns, timeout=None):
        """
        ``patterns`` have the form::

          {a_tag: a_method_name,
           a_tag_2: a_function,
           ...}

        Special tags are:

        * ``*``: matches any tag
        * ``timeout``: is executed when a ``receive`` times out
        
        """
        inbox_polling = timeout and self.INBOX_POLLING_TIMEOUT
        processed = Queue.Queue()
        start_time = time.time()
        msg = {}
        while True:
            current_time = time.time()
            if timeout is not None and current_time >= start_time + timeout:
                matched = 'timeout'
                break
            try:
                msg = self.inbox.get(True, inbox_polling)
            except Queue.Empty:
                continue
            if msg['tag'] in patterns:
                matched = msg['tag']
                break
            if '*' in patterns:
                matched = '*'
                break
            processed.put(msg)
        try:
            action = patterns[matched]
        except KeyError:
            return
        if isinstance(action, str):
            f = getattr(self, action)
        elif callable(action):
            f = action
        while not processed.empty():
            self.inbox.put(processed.get())
        f(msg)

    def act(self):
        """
        Subclasses must implement this method.
        """
        raise NotImplementedError

class ThreadedActor(Actor):
    """
    A threaded version of an actor.  It runs as a daemon thread.
    """
    
    def __init__(self, name):
        super(ThreadedActor, self).__init__(name)
        self.thread = threading.Thread(target=self.act)
        self.thread.daemon = True
        self.thread.start()

class ProcessActor(Actor):
    """
    An actor running in an independent process.
    """

    def __init__(self, name):
        super(ProcessActor, self).__init__(name)
        self.proc = m.Process(target=self.act)
        self.proc.daemon = True
        self.proc.start()

    def kill(self):
        """
        Kill the process containing the actor.
        """
        pid = self.proc.pid
        self.proc.terminate()
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass

class TimeoutTest(Actor):

    def act(self):
        self.receive({'test': 'test'},
                     timeout=5)
        
class Test(ProcessActor):

    def test(self, msg):
        print 'tag: test', msg
        
    def foo(self, msg):
        print 'tag: foo', msg

    def queue(self, msg):
        print 'msgs in queue:', self.inbox.qsize()

    def reply_me(self, msg):
        sender = msg['reply_to']
        q = self.get_inbox(sender)
        q.put({'tag': 'ack', 'answer': 5})
        
    def act(self):
        x = 3
        while True:
            self.receive(
                {'test': 'test',
                 'foo': 'foo',
                 'queue': 'queue',
                 'fun': lambda msg: sys.stdout.write('--> %s\n' %x),
                 'reply_me': 'reply_me',
                 '*': lambda msg: sys.stdout.write('any other stuff\n')}
                )
            print 'After receive'
            
class SyncTest(ProcessActor):

    def act(self):
        q = self.get_inbox('t')
        q.put({'tag': 'reply_me', 'reply_to': self.name})
        print 'Sent "reply_me"'
        self.receive(
            {'ack': 'answer'})

    def answer(self, msg):
        print 'Got answer', msg['answer']
        print 'And now will wait for a message "foo"'
        self.receive(
            {'foo': 'done'})

    def done(self, msg):
        print "ok, I'm done"
        
