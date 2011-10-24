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
from pycommon import log

# multiproc_logger = m.log_to_stderr()
# multiproc_logger.setLevel(m.SUBDEBUG)

logger = log.setup('actor', 'to_console')

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
        self.q.destroy_ref()

    def destroy_actor(self):
        self.q.destroy_queue()

    def flush(self):
        self.q.flush()

    def __del__(self):
        try:
            logger.debug('Finalizing actor_ref for %s' %self.name)
            self.destroy_ref()
        except:
            pass
        
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
        logger.debug('[Actor %s] creating my inbox' %(self.name,))
        self.inbox = manager.QueueRef(self.name)
        logger.debug('[Actor %s] getting the created inbox' %(self.name,))
        if self.name == 'hardware':
            self.my_log = lambda *args, **kwargs: None
        else:
            self.my_log = logger.debug

    def __del__(self):
        try:
            logger.debug('Finalizing actor %s' %self.name)
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
        inbox_polling = timeout and self.INBOX_POLLING_TIMEOUT
        processed = Queue.Queue()
        start_time = current_time = time.time()
        msg = {}
        self.my_log('[Actor %s] starting receive'%(self.name,))
        starting_size = self.inbox.qsize()
        checked_objects = 0
        while True:
            # Process any pre-existing objects in the queue, before
            # starting to consider the timeout
            if (checked_objects >= starting_size
                    and timeout is not None
                    and current_time > start_time + timeout):
                matched = 'timeout'
                break
            current_time = time.time()
            try:
                self.my_log('[Actor %s] checking inbox with timeout:'
                            '%s' %(self.name, inbox_polling))
                msg = self.inbox.get(timeout=inbox_polling)
                checked_objects += 1
                self.my_log('[Actor %s] got object: %s' %(self.name, msg))
                self.my_log('[...     ] in receive: %s' %(patterns,))
            except Queue.Empty:
                self.my_log('[Actor %s] empty inbox' %(self.name,))
                continue
            try:
                if not msg:
                    # Get a refreshed inbox and keep reading.  This
                    # may fail if the reason we lost connection is
                    # because the manager is stopping (for example
                    # when quitting the application).
                    self.inbox = manager.QueueRef(self.name)
                    continue
                if msg['tag'] in patterns:
                    matched = msg['tag']
                    break
                if '*' in patterns:
                    matched = '*'
                    break
            except (KeyError, TypeError):
                logger.debug('Wrong message object: %s' %(msg,))
                logger.debug('  discarding object...')
                continue
            # the object 'msg' was not matched, save it so we can
            # return it to the inbox
            processed.put(msg)
        # Return all unmatched objects to the inbox
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
    """
    A fast actor replaces its inbox (which is a connection to a queue
    in the manager) by a local queue, and starts a thread to read from
    manager into this new local queue.

    The ``receive`` function then reads from the local queue, which is
    faster.  The drawback is that we have to keep an additional
    thread.
    """

    def __init__(self, name=None, prefix=''):
        super(FastActor, self).__init__(name=name, prefix=prefix)
        logger.debug('[%s] Fast actor created' %self.name[:30])
        self.external = self.inbox
        self.inbox = Queue.Queue()
        self.t = threading.Thread(target=self.copy_to_internal)
        self.t.daemon = True
        self.t.start()

    def destroy_actor(self):
        q = ActorRef(self.external.name)
        q.destroy_actor()

    def copy_to_internal(self):
        try:
            while True:
                x = self.external.get()
                # getting None means we want to refresh the inbox by
                # flushing everything
                if x is None:
                    logger.debug('Thread got None. Refreshing queue')
                    self.external.flush()
                    self.inbox = Queue.Queue()
                # getting False means that the socket is closing, just
                # leave so the thread finishes
                elif x is False:
                    logger.debug('Thread got False')
                    return
                else:
                    self.inbox.put(x)
        except Exception as exc:
            # when actor dies, queue will get eof
            # just leave
            pass
        
class Test(Actor):
    def act(self):
        self.receive({'foo': lambda *args: None})
        
