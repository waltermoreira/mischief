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

    IP = 'localhost'
    PORT = 5123
    
    def __init__(self, *args, **kwargs):
        kwargs['address'] = (self.IP, self.PORT)
        kwargs['authkey'] = 'actor'
        super(ActorManager, self).__init__(*args, **kwargs)
        self.register('create_queue', callable=self.create_queue)
        self.register('get_named', callable=self.get_named)
        self.register('destroy_named', callable=self.destroy_named)
        self.named_queues = {}
        actor_logger.debug('[ActorManager] Created with pid: %s' %m.current_process().pid)

    def create_queue(self, name):
        actor_logger.debug('[ActorManager] <create> queue %s' %name)
        actor_logger.debug('[ActorManager]   len(named_queues) = %d' %len(self.named_queues))
        self.named_queues[name] = Queue.Queue()

    def get_named(self, name):
        return self.named_queues[name]

    def destroy_named(self, name):
        actor_logger.debug('[ActorManager] <destroy> queue %s' %name)
        try:
            actor_logger.debug('[ActorManager] Before: len named_queues = %s' %len(self.named_queues))
            del self.named_queues[name]
            actor_logger.debug('[ActorManager] After: len named_queues = %s' %len(self.named_queues))
        except KeyError:
            pass

    def get_actor_ref(self, name):
        """
        Get a reference to an actor::

           x = m.get_actor_ref('foo')
           x.send(...)
           
        """
        return ActorRef(self.get_named(name), name)

    def start(self):
        try:
            socket.create_connection((self.IP, self.PORT))
            # manager already started, just connect to it
            self.connect()
        except socket.error:
            # start a manager
            super(ActorManager, self).start()
            actor_logger.debug('[ActorManager] Started')
        
    def stop(self):
        actor_logger.debug('[ActorManager] Stopped')
        print 'Finalizing'
        # for q in self.named_queues.values():
        #     q.close()
        self.shutdown()
        
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
    
    def __init__(self, name=None, prefix=''):
        self.name = name or ('actor_' + prefix + '_' +
                             str(uuid.uuid1().hex))
        self.qm = ActorManager()
        self.qm.connect()
        self.qm.create_queue(self.name)
        self.inbox = self.qm.get_named(self.name)

    def __del__(self):
        try:
            self.qm.destroy_named(self.name)
        except Exception as exc:
            pass
        
    def me(self):
        return self.name
    
    def get_inbox(self, name):
        return self.qm.get_named(name)

    def send(self, to, msg):
        self.get_inbox(to).put(msg)

    def get_actor_ref(self, name):
        """
        Get a reference to actor named ``name``.
        """
        return self.qm.get_actor_ref(name)

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
        if timeout == 0 and self.inbox.qsize() == 0:
            # optimization: this is the usual case when called from
            # C++
            return
        inbox_polling = timeout and self.INBOX_POLLING_TIMEOUT
        processed = Queue.Queue()
        start_time = current_time = time.time()
        msg = {}
        while True:
            if timeout is not None and current_time > start_time + timeout:
                matched = 'timeout'
                break
            current_time = time.time()
            try:
                msg = self.inbox.get(True, inbox_polling)
            except EOFError:
                # inbox queue was closed
                # actor exits
                os._exit(0)
            except Queue.Empty:
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

    def __init__(self, prefix=''):
        super(FastActor, self).__init__(prefix=prefix)
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
        
