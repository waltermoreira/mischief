"""
:mod:`het2_common.actors.actor` -- Actor library
=============================================

.. module:: actor
.. moduleauthor:: Walter Moreira <moreira@astro.as.utexas.edu>

An implementation of the `actor model`_.

An actor inherits from ``ThreadedActor`` or ``ProcessActor``, and it
implements the method ``act()``::

  class MyActor(ThreadedActor):
      def act(self):
          self.receive({
              'foo': lambda msg: None
              })

.. _actor model: http://en.wikipedia.org/wiki/Actor_model
"""

import pprint
import multiprocessing as m
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
import inspect
import psutil
from pipe import Pipe
from het2_common import log

logger = log.setup('actor', 'to_file')

class ActorException(Exception):
    pass
    
class ActorRef(object):
    """
    An actor reference.
    """
    
    def __init__(self, name):
        self.name = name
        self.q = Pipe(name, 'w')
        self._tag = None

    def __getattr__(self, attr):
        self._tag = attr
        return self

    def __enter__(self):
        """
        An actor reference is also a context manager. It destroys
        itself on exit.

        Use as::

            with ActorRef('foo') as foo:
                ...
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        ActorRef destroy itself if used as a context manager.
        """
        self.destroy_ref()
        
    def __call__(self, *args, **kwargs):
        if self._tag is None:
            raise TypeError("actor is not callable")
        msg = {'tag': self._tag}
        msg.update(kwargs)
        self.send(msg)
        self._tag = None
            
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
        if self.q is not None:
            self.q.close()

    def destroy_actor(self):
        if self.q is not None:
            self.q.destroy()

    def __del__(self):
        try:
            print('Finalizing actor_ref for %s' %self.name)
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
                             str(uuid.uuid1().hex) + '_' + self.my_id(inspect.stack()[2]))
        print 'Creating actor with inbox', self.name
        print('[Actor %s] creating my inbox' %(self.name,))
        self.inbox = Pipe(self.name, 'r')

    def my_id(self, frame_record):
        frame, f, lineno, fun = frame_record[0:4]
        return '%s_%s_%s' %(os.path.basename(f), lineno, fun)
        
    def __del__(self):
        try:
            print('Finalizing actor %s' %self.name)
            self.destroy_actor()
        except:
            pass

    def me(self):
        return self.name

    def destroy_actor(self):
        self.inbox.destroy()
        
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
        print('[Actor %s] creating temporary queue' %(self.name,))
        processed = Queue.Queue()
        print('[Actor %s] Temporary queue created' %(self.name,))
        start_time = current_time = time.time()
        msg = {}
        print('[Actor %s] starting receive'%(self.name,))
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
                print('[Actor %s] checking inbox with timeout:'
                            '%s' %(self.name, inbox_polling))
                msg = self.inbox.get(timeout=inbox_polling)
                checked_objects += 1
                print('[Actor %s] got object: %s' %(self.name, msg))
                print('[...     ] in receive: %s' %(patterns,))
            except Queue.Empty:
                print('[Actor %s] empty inbox' %(self.name,))
                continue
            try:
                if msg.get('_internal'):
                    # Special messages for internal operation
                    self.process_internal(msg)
                if msg['tag'] in patterns:
                    matched = msg['tag']
                    break
                if '*' in patterns:
                    matched = '*'
                    break
            except (KeyError, TypeError):
                print('Wrong message object: %s' %(msg,))
                print('  discarding object...')
                continue
            # the object 'msg' was not matched, save it so we can
            # return it to the inbox
            processed.put(msg)
        # Return all unmatched objects to the inbox
        while not processed.empty():
            x = processed.get()
            print('restoring object: %s' %(x,))
            # restore unprocessed object directly to the reader queue,
            # bypassing the fifo, to avoid overhead.  This is possible
            # because we have a reference to the read end of the
            # pipe.
            self.inbox.reader_queue.put(x)
        try:
            action = patterns[matched]
        except KeyError:
            return
        if isinstance(action, str):
            f = getattr(self, action)
        elif callable(action):
            f = action
        f(msg)

    def process_internal(self, msg):
        if msg['tag'] == '_quit':
            print('[Actor %s] got __quit')
            # Special token to unlock actors.  Raise
            # 'StopIteration' to break loops where the receive
            # function is.
            raise StopIteration
        if msg['tag'] == '_ping':
            with ActorRef(msg['reply_to']) as sender:
                sender._pong()
            
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

class Echo(ThreadedActor):
    """
    Convenience actor to display responses from other actors.

    Use by directing 'reply_to' to this actor.
    """
    
    def __init__(self):
        super(Echo, self).__init__(name='echo')
        
    def act(self):
        while True:
            self.receive({'*': self.echo})

    def echo(self, msg):
        print '[Echo]'
        pprint.pprint(msg, width=1)
