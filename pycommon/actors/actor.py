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
    
    def __init__(self, *args, **kwargs):
        kwargs['address'] = ('localhost', 5123)
        kwargs['authkey'] = 'actor'
        super(ActorManager, self).__init__(*args, **kwargs)
        self.register('create_queue', callable=self.create_queue)
        self.register('get_named', callable=self.get_named)
        self.register('destroy_named', callable=self.destroy_named)
        self.named_queues = {}
        actor_logger.debug('[ActorManager] Created with pid: %s' %m.current_process().pid)

    def create_queue(self, name):
        actor_logger.debug('[ActorManager] create queue %s' %name)
        actor_logger.debug('[ActorManager]   len(named_queues) = %d' %len(self.named_queues))
        self.named_queues[name] = Queue.Queue()

    def get_named(self, name):
        actor_logger.debug('[ActorManager] get named %s' %name)
        return self.named_queues[name]

    def destroy_named(self, name):
        actor_logger.debug('[ActorManager] destroy %s' %name)
        try:
            actor_logger.debug('[ActorManager] Before: len named_queues = %s' %len(self.named_queues))
            del self.named_queues[name]
            actor_logger.debug('[ActorManager] After: len named_queues = %s' %len(self.named_queues))
        except KeyError:
            actor_logger.debug('[ActorManager] KeyError')
            pass

    def get_actor_ref(self, name):
        """
        Get a reference to an actor::

           x = m.get_actor_ref('foo')
           x.send(...)
           
        """
        actor_logger.debug('[ActorManager] asked for new reference for %s' %name)
        return ActorRef(self.get_named(name), name)

    def start(self):
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
        actor_logger.debug("[ref-%s] I'm a new reference for actor %s" %(self.name[:30], name))
        
    def send(self, msg):
        """
        Send a message to the actor represented by this reference.
        """
        actor_logger.debug('[ref-%s] Sent message: %s' %(self.name[:30], msg))
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
        actor_logger.debug('[%s] Actor created' %self.name)

    def __del__(self):
        self.qm.destroy_named(self.name)
        
    def me(self):
        return self.name
    
    def get_inbox(self, name):
        return self.qm.get_named(name)

    def send(self, to, msg):
        actor_logger.debug('[%s] Actor sent to %s the message: %s' %(self.name[:30],
                                                                     to, msg))
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
        actor_logger.debug('[%s] Starting receive with timeout = %s' %(self.name[:30],
                                                                       inbox_polling))
        actor_logger.debug('[%s]   Will listen to: %s' %(self.name[:30], patterns.keys()))
        while True:
            if timeout is not None and current_time > start_time + timeout:
                matched = 'timeout'
                actor_logger.debug('[%s]   Matched timeout' %self.name[:30])
                break
            current_time = time.time()
            try:
                msg = self.inbox.get(True, inbox_polling)
                actor_logger.debug('[%s]   Got message: %s' %(self.name[:30], msg))
            except EOFError:
                # inbox queue was closed
                # actor exits
                actor_logger.debug('[%s]   Got EOFError, quitting' %self.name[:30])
                os._exit(0)
            except Queue.Empty:
                actor_logger.debug('[%s]   Queue is empty, polling again' %self.name[:30])
                continue
            if msg['tag'] in patterns:
                actor_logger.debug('[%s]   Matched %s' %(self.name[:30], msg['tag']))
                matched = msg['tag']
                break
            if '*' in patterns:
                actor_logger.debug('[%s]   Matched *' %self.name[:30])
                matched = '*'
                break
            actor_logger.debug('[%s]   Nothing matched, saving msg "%s" to internal queue' %(self.name[:30], msg['tag']))
            processed.put(msg)
        actor_logger.debug('[%s] We matched: %s. Restoring internal queue to inbox (%s items)' %(self.name[:30], matched, processed.qsize()))
        while not processed.empty():
            x = processed.get()
            self.inbox.put(x)
            actor_logger.debug('[%s]   Restored: %s' %(self.name[:30], x['tag']))
        try:
            action = patterns[matched]
            actor_logger.debug('[%s]   Found action' %self.name[:30])
        except KeyError:
            actor_logger.debug('[%s] No action found, just finishing receive' %self.name[:30])
            return
        if isinstance(action, str):
            f = getattr(self, action)
        elif callable(action):
            f = action
        actor_logger.debug('[%s] About to execute action %s' %(self.name[:30], f.func_name))
        f(msg)
        actor_logger.debug('[%s] Finished executing action %s' %(self.name[:30], f.func_name))

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

class ProcessActor(Actor):
    """
    An actor running in an independent process.
    """

    def __init__(self, name=None, prefix=''):
        super(ProcessActor, self).__init__(name=name,
                                           prefix=prefix)
        actor_logger.debug('[%s] I am pid: %s' %(self.name[:30], m.current_process().pid))
        parent_conn, child_conn = m.Pipe()
        actor_logger.debug('[%s] Connections: %s, %s' %(self.name[:30],
                                                        parent_conn, child_conn))
        self.proc = m.Process(target=self.child,
                              args=(child_conn,))
        actor_logger.debug('[%s] Starting self detaching process' %self.name[:30])
        self.proc.start()
        actor_logger.debug('[%s] Waiting for pid' %self.name[:30])
        self.pid = parent_conn.recv()
        actor_logger.debug('[%s] Got pid: %s' %(self.name[:30], self.pid))
        child_conn.close()
        # self.proc.join()
        # actor_logger.debug('[%s] Joined detacher, we are leaving' %self.name[:30])
        
    def child(self, ch):
        actor_logger.debug('[%s] Detacher is starting child that "acts"' %self.name[:30])
        actor_logger.debug('[%s] I am detacher pid: %s' %(self.name[:30], m.current_process().pid))
        self.p = m.Process(target=self.clean_act)
        self.p.start()
        ch.send(self.p.pid)
        actor_logger.debug('[%s] Child that "acts" started' %self.name[:30])
        actor_logger.debug('[%s]   Sent back to parent pid = %d' %(self.name[:30], self.p.pid))
        actor_logger.debug('[%s]   Exiting' %self.name[:30])
        os._exit(0)

    def clean_act(self):
        try:
            actor_logger.debug('[%s] Starting clean act' %self.name[:30])
            actor_logger.debug('[%s] I am the actor pid: %s' %(self.name[:30], m.current_process().pid))
            self.act()
            actor_logger.debug('[%s] Finishing clean act' %self.name[:30])
        finally:
            actor_logger.debug('[%s] Destroying queue in manager' %self.name[:30])
            self.qm.destroy_named(self.name)
            actor_logger.debug('[%s] Leaving clean_act' %self.name[:30])
            
    def kill(self):
        """
        Kill the process containing the actor.
        """
        pid = self.pid
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass

class FastActor(Actor):

    def __init__(self, prefix=''):
        super(FastActor, self).__init__(prefix=prefix)
        actor_logger.debug('[%s] Fast actor created' %self.name[:30])
        self.external = self.inbox
        self.inbox = Queue.Queue()
        self.t = threading.Thread(target=self.copy_to_internal)
        self.t.daemon = True
        self.t.start()
        actor_logger.debug('[%s]   Helper thread for ast actor started' %self.name[:30])

    def copy_to_internal(self):
        try:
            while True:
                self.inbox.put(self.external.get())
        except EOFError:
            # when actor dies, queue will get eof
            # just leave
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
                 'sync': self.read_value('sync_value'),
                 'fun': lambda msg: sys.stdout.write('--> %s\n' %x),
                 'reply_me': 'reply_me'})
                #  '*': lambda msg: sys.stdout.write('any other stuff\n')}
                # )
            print 'After receive'
            try:
                print '>>>>', self.sync_value
            except:
                print 'sync_value not set'
            
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
        
