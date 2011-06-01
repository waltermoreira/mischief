"""
:mod:`pycommon.actors.actor` -- Actor library
=============================================

.. module:: actor
.. moduleauthor:: Walter Moreira <moreira@astro.as.utexas.edu>

An implementation of the `actor model`_.

A queue manager must be created somewhere, and started::

  qm = QueueManager(address=('localhost', 5000), authkey='actor')
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

class ConnectionManager(managers.BaseManager):
    pass

class QueueManager(managers.BaseManager):

    def __init__(self, *args, **kwargs):
        super(QueueManager, self).__init__(*args, **kwargs)
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

class Actor(object):
    """
    Messages to the actor have the form::

      {'tag': 'foo',
       ...}
    """
    def __init__(self, name):
        self.name = name
        self.qm = QueueManager(address=('localhost', 5000), authkey='actor')
        self.qm.connect()
        self.qm.create_queue(self.name)
        self.inbox = self.qm.get_named(self.name)

    def get_inbox(self, name):
        return self.qm.get_named(name)

    def send(self, to, msg):
        self.get_inbox(to).put(msg)

    def receive(self, patterns):
        """
        patterns have the form::

          {a_tag: a_method, ...}
        """
        processed = Queue.Queue()
        while True:
            msg = self.inbox.get()
            if msg['tag'] in patterns:
                matched = msg['tag']
                break
            if '*' in patterns:
                matched = '*'
                break
            processed.put(msg)
        action = patterns[matched]
        if isinstance(action, str):
            f = getattr(self, action)
        elif callable(action):
            f = action
        while not processed.empty():
            self.inbox.put(processed.get())
        f(msg)

    def act(self):
        pass

class ThreadedActor(Actor):

    def __init__(self, name):
        super(ThreadedActor, self).__init__(name)
        self.thread = threading.Thread(target=self.act)
        self.thread.daemon = True
        self.thread.start()

class ProcessActor(Actor):

    def __init__(self, name):
        super(ProcessActor, self).__init__(name)
        self.proc = m.Process(target=self.act)
        self.proc.daemon = True
        self.proc.start()
        
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
                 '*': lambda msg: sys.stdout.write('any other stuff\n')})
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
        
