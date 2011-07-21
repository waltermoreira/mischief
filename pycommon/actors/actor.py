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
import manager
import threading
import logging
import logging.handlers
import Queue
import sys
import os
import signal
import time
import uuid
import socket

# multiproc_logger = m.log_to_stderr()
# multiproc_logger.setLevel(m.SUBDEBUG)

file_logger = logging.getLogger('file')
actor_logger = logging.getLogger('actor')
handler = logging.handlers.RotatingFileHandler(os.path.join(os.environ['HET2_DEPLOY'], 'log/gui', 'actor.log'))
formatter = logging.Formatter("%(asctime)s %(levelname)-8s [%(filename)30s:%(lineno)-4s] - %(message)s")
handler.setFormatter(formatter)
actor_logger.addHandler(handler)
actor_logger.setLevel(logging.DEBUG)

        
class ActorRef(object):
    """
    An actor reference.
    """
    
    def __init__(self, name):
        self.name = name
        self.q = manager.QueueRef(name)
        
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

    def destroy_ref(self):
        print 'destroying ref for', self.name
        self.q.destroy_ref()

    def destroy_actor(self):
        print 'destroying actor', self.name
        self.q.destroy_queue()

    def __del__(self):
        pass
        # self.destroy_ref()
        
class Actor(object):
    """
    Messages to the actor have the form::

      {'tag': 'foo',
       ...}
    """

    # When a timeout is given in a ``receive``, check every
    # ``INBOX_POLLING_TIMEOUT`` seconds whether we have timed out.
    INBOX_POLLING_TIMEOUT = 0.01
    
    def __init__(self, name=None, prefix=''):
        self.name = name or ('actor_' + prefix + '_' +
                             str(uuid.uuid1().hex))
        actor_logger.debug('[Actor %s] creating my inbox' %(self.name,))
        self.inbox = manager.QueueRef(self.name)
        actor_logger.debug('[Actor %s] getting the created inbox' %(self.name,))
        if self.name == 'hardware':
            self.my_log = lambda *args, **kwargs: None
        else:
            self.my_log = actor_logger.debug

    def __del__(self):
        try:
            self.destroy_actor()
        except:
            pass
        
    def me(self):
        return self.name

    def send(self, msg):
        """
        Send to myself
        """
        q = ActorRef(self.name)
        q.send(msg)
        q.destroy_ref()
        
    def destroy_actor(self):
        self.inbox.destroy_queue()
        
    def read_value(self, value_name):
        def _f(msg):
            setattr(self, value_name, msg[value_name])
        return _f
    
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
        # if timeout == 0 and self.inbox.qsize() == 0:
        #     # optimization: this is the usual case when called from
        #     # C++
        #     return
        inbox_polling = timeout and self.INBOX_POLLING_TIMEOUT
        processed = Queue.Queue()
        start_time = current_time = time.time()
        msg = {}
        self.my_log('[Actor %s] starting receive'%(self.name,))
        while True:
            if timeout is not None and current_time > start_time + timeout:
                matched = 'timeout'
                break
            current_time = time.time()
            try:
                self.my_log('[Actor %s] checking inbox with timeout: %s'%(self.name, inbox_polling))
                msg = self.inbox.get(timeout=inbox_polling)
                if type(msg) != dict:
                    actor_logger.debug('[Actor %s] got msg: %s' %(self.name, msg))
                self.my_log('[Actor %s] got object: %s' %(self.name, msg))
            except Queue.Empty:
                self.my_log('[Actor %s] empty inbox' %(self.name,))
                continue
            if msg['tag'] in patterns:
                matched = msg['tag']
                break
            if '*' in patterns:
                matched = '*'
                break
            processed.put(msg)
        while not processed.empty():
            x = processed.get()
            self.inbox.put(x)
        try:
            action = patterns[matched]
        except KeyError:
            return
        if isinstance(action, str):
            f = getattr(self, action)
        elif callable(action):
            f = action
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
    
    def __init__(self, name=None, prefix=''):
        super(ThreadedActor, self).__init__(name=name,
                                            prefix=prefix)
        self.thread = threading.Thread(target=self.act)
        self.thread.daemon = True
        self.thread.start()

class FastActor(Actor):

    def __init__(self, name=None, prefix=''):
        super(FastActor, self).__init__(name=name, prefix=prefix)
        actor_logger.debug('[%s] Fast actor created' %self.name[:30])
        self.external = self.inbox
        self.inbox = Queue.Queue()
        self.t = threading.Thread(target=self.copy_to_internal)
        self.t.daemon = True
        self.t.start()

    def copy_to_internal(self):
        try:
            while True:
                self.inbox.put(self.external.get())
        except EOFError:
            # when actor dies, queue will get eof
            # just leave
            pass
        
