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
from pipe import Receiver, Sender
from het2_common import log

logger = log.setup('actor', 'to_file')

class ActorException(Exception):
    pass

class ActorFinished(Exception):
    pass
    
class ActorRef(object):
    """
    An actor reference.
    """
    
    def __init__(self, name):
        self.name = name
        self.sender = Sender(name)
        self._tag = None

    def is_alive(self):
        """
        Send a ping to the associated actor and wait for a pong
        """
        with _ListenerActor() as listener:
            self._ping(reply_to=listener.name)
            listener.wait_pong(timeout=0.5)
            return listener.pong
        
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
        self.close()
        
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
        self.sender.put(msg)

    def close(self):
        """
        Close just the reference
        """
        self.sender.close()

    def close_actor(self, confirm_to=None):
        """
        Remotely stop the actor with the internal message '_quit'
        """
        confirm_msg = {'tag': 'closed'}
        self.sender.close_receiver(confirm_to=confirm_to, confirm_msg=confirm_msg)

        
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
        try:
            self.name = name or '_'.join(filter(
                None, ['a', prefix, str(uuid.uuid1().hex)[:8],
                       self.my_id(inspect.stack()[2])]))
        except Exception as exc:
            self.name = '_'.join(filter(
                None, ['a', prefix, str(uuid.uuid1().hex)]))
        self.inbox = Receiver(self.name)

    def my_id(self, frame_record):
        frame, f, lineno, fun = frame_record[0:4]
        return '%s_%s_%s_%s' %(os.path.basename(f), lineno, fun, os.getpid())
	         
    def __enter__(self):
        """
        Actor can be used as a context
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Actor context closes it on exit
        """
        with ActorRef(self.name) as myself:
            myself.close_actor()
    
    def close(self, confirm_to=None):
        confirm_msg = {'tag': 'closed'}
        self.inbox.close(confirm_to, confirm_msg)
    
    def read_value(self, value_name):
        def _f(msg):
            setattr(self, value_name, msg[value_name])
        return _f
    
    def receive(self, patterns=None, timeout=None, **more_patterns):
        """
        ``patterns`` have the form::

          {a_tag: a_method_name,
           a_tag_2: a_function,
           ...}

        Special tags are:

        * ``*``: matches any tag
        * ``timeout``: is executed when a ``receive`` times out
        
        """
        if patterns is None:
            patterns = {}
        patterns.update(more_patterns)
        
        inbox_polling = timeout and self.INBOX_POLLING_TIMEOUT
        processed = Queue.Queue()
        start_time = current_time = time.time()
        msg = {}
        starting_size = self.inbox.qsize()
        checked_objects = 0
        while True:
            # Process any pre-existing objects in the queue, before
            # starting to consider the timeout
            if (checked_objects >= starting_size
                    and timeout is not None
                    and current_time > start_time + timeout):
                matched = 'timed_out'
                break
            current_time = time.time()
            try:
                msg = self.inbox.get(timeout=inbox_polling)
                if msg is None:
                    raise ActorFinished()
                checked_objects += 1
            except Queue.Empty:
                continue
            try:
                if msg['tag'] == '_ping':
                    # Special handler for _ping, since we don't want
                    # to break the loop
                    self._pong(msg)
                    continue
                if msg['tag'] in patterns:
                    matched = msg['tag']
                    break
                if '_' in patterns:
                    matched = '_'
                    break
            except (KeyError, TypeError):
                continue
            # the object 'msg' was not matched, save it so we can
            # return it to the inbox
            processed.put(msg)
        # Return all unmatched objects to the inbox
        while not processed.empty():
            x = processed.get()
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
            # a string means a method of self (for those cases the
            # method is added later)
            f = getattr(self, action)
        elif callable(action):
            # a callable can be methods or functions
            f = action
        elif action is None:
            # None is a shortcut for an empty handler
            f = lambda msg: None
        f(msg)

    def _pong(self, msg):
        """
        Special method to respond to a ping
        """
        with ActorRef(msg['reply_to']) as sender:
            sender._pong()
            
    def act(self):
        """
        Subclasses must implement this method.
        """
        raise NotImplementedError

    # def __del__(self):
    #     try:
    #         self.close()
    #     except Exception:
    #         pass
        
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
            self.receive(
                _ = self.echo)

    def echo(self, msg):
        print '[Echo]'
        pprint.pprint(msg, width=1)

class _ListenerActor(Actor):
    """
    Utility actor to wait for pongs, when an actor ref sends a
    ping
    """
    
    def wait_pong(self, timeout=0):
        self.receive(
            _pong = self.got_pong,
            timed_out = self.timed_out,
            timeout = timeout)

    def got_pong(self, msg):
        self.pong = True

    def timed_out(self, msg):
        self.pong = False