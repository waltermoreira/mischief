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
import threading
import queue
import sys
import os
import time
import uuid
import inspect

from .pipe import Receiver, Sender, is_local_ip, get_local_ip
from ..log import setup
from ..exceptions import ActorFinished, PipeEmpty, PipeException
from ..tools import Addressable

logger = setup(to=['file', 'console'])

class ActorRef(Addressable):
    """
    An actor reference.
    """
    
    def __init__(self, address):
        if isinstance(address, Addressable):
            self._address = address.address()
        elif isinstance(address, str):
            self._address = (address, 'localhost', None)
        else:
            self._address = address
        self.name, self.ip, self.port = self._address
        self.sender = Sender(self._address)
        self._tag = None

    def address(self):
        return self._address
        
    def sync(self, tag, **kwargs):
        """
        Utility to send a message synchronously
        """
        with _ReplyWaiter() as waiter:
            kwargs['tag'] = tag
            kwargs['reply_to'] = waiter
            self.send(kwargs)
            return waiter.act()
            
    def is_alive(self):
        """
        Send a ping to the associated actor and wait for a pong
        """
        with _ListenerActor() as listener:
            self.send({'tag': '__ping__',
                       'reply_to': listener})
            listener.wait_pong(timeout=0.5)
            return listener.pong

    def full_address(self):
        with _ReplyWaiter() as waiter:
            self.send({'tag': '__address__',
                       'reply_to': waiter})
            resp = waiter.act()
            return resp['address'], resp['pid']
        
    def __getattr__(self, attr):
        if attr.startswith('_') or attr == 'trait_names':
            raise AttributeError(attr)
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
        reply_to = msg.get('reply_to')
        if isinstance(reply_to, Addressable):
            name, ip, port = reply_to.address()
            if is_local_ip(ip):
                ip = get_local_ip(self.ip)
            msg['reply_to'] = (name, ip, port)
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


def gen_name():
    return str(uuid.uuid1().hex)
    
class Actor(Addressable):
    """
    Messages to the actor have the form::

      {'tag': 'foo',
       ...}
    """

    # When a timeout is given in a ``receive``, check every
    # ``INBOX_POLLING_TIMEOUT`` seconds whether we have timed out.
    INBOX_POLLING_TIMEOUT = 0.01
    
    def __init__(self, name=None, ip='localhost'):
        self.name = name or gen_name()
        self.ip = ip
        self.inbox = Receiver(self.name, self.ip)

    def address(self):
        return self.inbox.address()

    def __enter__(self):
        """
        Actor can be used as a context
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Actor context closes it on exit
        """
        try:
            with ActorRef(self.address()) as myself:
                myself.close_actor()
        except PipeException:
            # If actor was already closed, ignore error from the
            # reference trying to ping the actor
            pass
            
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
        processed = queue.Queue()
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
            except PipeEmpty:
                continue
            try:
                if msg['tag'] == '_debug':
                    # Special handler for _debug, since we don't want
                    # to break the loop
                    self._debug(msg, patterns)
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
        f(AttributeDict(msg))

    def _debug(self, msg, patterns):
        """Special method to respond to a _debug message.

        Answer the patterns of the receive where the actor is when
        receiving the message.

        """
        with ActorRef(msg['reply_to']) as sender:
            p = {key:repr(val) for key, val in patterns.items()}
            sender.debug_reply(patterns=p)
            
    def act(self):
        """
        Subclasses must implement this method.
        """
        raise NotImplementedError

class AttributeDict(dict):
    """A dict that can access keys via attributes"""

    def __getattr__(self, attr):
        return self[attr]

    def __setattr__(self, attr, val):
        self[attr] = val    

class ThreadedActor(Actor):
    """
    A threaded version of an actor.  It runs as a daemon thread.
    """
    
    def __init__(self, name=None, ip='localhost', **kwargs):
        super(ThreadedActor, self).__init__(name, ip)
        self.__dict__.update(kwargs)
        self.thread = threading.Thread(target=self.threaded_act)
        self.thread.daemon = True
        self.thread.start()

    def threaded_act(self):
        try:
            self.act()
        except ActorFinished:
            pass
            
    @staticmethod
    def spawn(actor, name=None, ip='localhost', **kwargs):
        """Convenience function for symmetry with process actors."""
        return actor(name, ip, **kwargs)
    
class Echo(ThreadedActor):
    """
    Convenience actor to display responses from other actors.

    Use by directing 'reply_to' to this actor.
    """
    
    def __init__(self, ip='localhost'):
        super(Echo, self).__init__(name='echo', ip=ip)
        
    def act(self):
        while True:
            self.receive(
                _ = self.echo)

    def echo(self, msg):
        print('[Echo]')
        pprint.pprint(msg, width=1)

class _ListenerActor(Actor):
    """
    Utility actor to wait for pongs, when an actor ref sends a
    ping
    """
    
    def wait_pong(self, timeout=0):
        self.receive(
            __pong__ = self.got_pong,
            timed_out = self.timed_out,
            timeout = timeout)

    def got_pong(self, msg):
        self.pong = True

    def timed_out(self, msg):
        self.pong = False

class _ReplyWaiter(Actor):

    def act(self):
        self.receive(reply=self.read_reply)
        return self.reply

    def read_reply(self, msg):
        self.reply = msg


def spawn(actor, **kwargs):
    return actor.spawn(actor, **kwargs)