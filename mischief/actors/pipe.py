from itertools import izip_longest, imap
from Queue import Queue
import time
import struct
import uuid
import json
import errno
import os
import threading
import traceback
import zmq
import socket
import inspect
from collections import defaultdict
from contextlib import contextmanager
from .. import log

logger = log.setup(to=['file', 'console'])

Context = zmq.Context()
ACTORS_DIRECTORY = '/tmp/actors_%s' %os.environ.get('USER', 'NO_USER')
try:
    # create directory to put the named pipes
    os.makedirs(ACTORS_DIRECTORY)
except OSError as exc:
    # ignore error if directory already exists
    if exc.errno != errno.EEXIST:
        raise

# Set some time to wait for sockets to deliver messages
# We don't want infinite time, since it may block when exiting
# the application
Context.linger = 5000 # ms

def path_to(name):
    """Path for posix ipc socket with name ``name``."""
    return os.path.join(ACTORS_DIRECTORY, name)

def get_local_ip(target):
    """Get the *local* ip.

    Warning: it doesn't mean the ``target`` would be able to connect
    to the ``local`` ip (for example, if the ``local`` machine is
    behind a NAT).

    """
    ipaddr = ''
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((target, 8000)) # 8000 is just a dummy number, it
                                  # doesn't actually connect to the
                                  # port
        ipaddr = s.getsockname()[0]
    finally:
        s.close()
    return ipaddr 

def is_local_ip(target):
    """Check that ``target`` is a local ip."""
    if target in (None, 'localhost', '127.0.0.1'):
        return True
    return target == get_local_ip(target)

    
@contextmanager
def zmq_socket(zmq_type):
    """Context manager for zmq sockets.

    Use as::

        with zmq_socket(zmq.REP) as s:
            ...
    
    """
    s = Context.socket(zmq_type)
    yield s
    s.close()

class PipeException(Exception):
    pass
    
class Receiver(object):
    """A receiver end of a pipe.

    Instantiate with ``Receiver(my_name)``.  It registers ``my_name``
    in the local namebroker.

    Use as::

        with Receiver('foo') as r:
            ...
            r.get()
            ...

    A receiver can get an special message that bypasses the fetching
    of the data, and it can be used to check the reader loop is
    running.  The message has the form::

        {'__tag__': '__ping__',
         'reply_to': sender_address}

    If the reader loop is running, it will respond with the message

        {'__tag__': '__pong__'}

    """
    def __init__(self, name):
        self.name = name
        self.path = path_to(name)

        self.reader_queue = Queue()
        self.reader_thread = threading.Thread(target=self._reader,
                                              args=(logger,))
        self.reader_thread.name = 'reader-%s'%(self.name,)
        self.reader_thread.daemon = True
        self.reader_thread.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def _reader_loop(self, socket):
        queue = self.reader_queue
        while True:
            data = socket.recv_json()
            try:
                __tag__ = data.get('__tag__')
                if __tag__ == '__quit__':
                    # means to shutdown the thread
                    # Close the socket just so the confirmation message
                    # goes after the socket is closed.  The 'with'
                    # statement of _reader would close it anyways.
                    socket.close()
                    # Put None in the queue to signal clients that are
                    # waiting for data
                    queue.put(None)
                    confirm_to = data.get('confirm_to', None)
                    if confirm_to is not None:
                        # Confirm that the socket was closed
                        with Sender(confirm_to) as sender:
                            confirm_msg = data.get('confirm_msg', None)
                            sender.put(confirm_msg)
                    return
                if __tag__ == '__ping__':
                    # answer special message without going to the receive,
                    # since the actor may be doing something long lasting
                    # and not reading the queue
                    with Sender(data['reply_to']) as sender:
                        sender.put({'__tag__': '__pong__'})
                    # avoid inserting this message in the queue
                    continue
                if __tag__ == '__low_level_ping__':
                    # answer a ping from a straight zmq socket
                    sender = data['reply_to']
                    with zmq_socket(zmq.PUSH) as s:
                        s.connect(sender)
                        s.send_json({'__tag__': '__pong__'})
                    continue
            except Exception:
                exc = traceback.format_exc()
                logger.debug('Reader thread for {} got an exception:'
                             .format(self.path))
                logger.debug(exc)
                data = {'__tag__': '__exception__',
                        'exception': exc}
            queue.put(data)
        
    def _reader(self, logger):
        """
        Thread function that reads the zmq socket and puts the data
        into the queue
        """
        with zmq_socket(zmq.PULL) as s:
            if os.name == 'posix':
                s.bind('ipc://%s' %self.path)
            port = s.bind_to_random_port('tcp://*')
            send_to_namebroker('localhost',
                               {'__tag__': 'register',
                                '__name__': self.name,
                                '__port__': port})
            self._reader_loop(s)
        
    def qsize(self):
        return self.reader_queue.qsize()
        
    def read(self, block=True, timeout=None):
        x = self.reader_queue.get(block, timeout)
        logger.debug('Receive at %s' %(self.name,))
        logger.debug('  message: %s' %(x,))
        return x

    def close(self, confirm_to=None, confirm_msg=None):
        with Sender(self.name) as sender:
            sender.close_receiver(confirm_to, confirm_msg)
        send_to_namebroker('localhost',
                           {'__tag__': 'unregister',
                            '__name__': self.name})
        
    # synonym
    get = read

    def __del__(self):
        self.close()


def send_to_namebroker(at, msg, timeout=1000):
    """Send message to NameBroker server at address ``at``."""
    with zmq_socket(zmq.REQ) as s:
        try:
            s.set(zmq.RCVTIMEO, timeout)
            s.connect('tcp://{}:{}'.format(at, NameBroker.PORT))
            s.send_json(msg)
            return s.recv_json()
        except zmq.Again:
            raise PipeException(
                'cannot connect to NameBroker at {}:{}'
                .format(at, NameBroker.PORT))

def get_port_for(name, at):
    """Consult namebroker for the port associated to a name."""
    resp = send_to_namebroker(at, {'__tag__': 'get',
                                   '__name__': name})
    return resp['__port__']

        
class Sender(object):
    """The sender end of a pipe.

    ``name`` can be

    - ``identifier``: a local pipe
    - ``ip:identifier``: a remote pipe

    A sender tries to use a local socket (ipc) if possible, unless the
    argument ``use_local`` is set to ``False``. In non-posix systems,
    it always use tcp.

    Use as::

        with Sender('foo') as s:
            ...
            s.put(msg)
            ...
    
    """

    def __init__(self, name, use_local=True):
        self.set_debug_name()
        self.ip, self.name = name.split(':') if ':' in name else (None, name)
        if os.name != 'posix' and self.ip is None:
            self.ip = 'localhost'
        if os.name != 'posix':
            self.local = False
        else:
            self.local = use_local and is_local_ip(self.ip)

        self.socket = Context.socket(zmq.PUSH)
            
        if self.local:
            self.path = path_to(self.name)
            logger.debug('Sender {} is using ipc'.format(name))
            self.socket.connect('ipc://%s' %self.path)
        else:
            port = get_port_for(self.name, at=self.ip)
            if port is None:
                raise PipeException('No port registered for {name} at {ip}'
                                    .format(ip=self.ip, name=self.name))
            logger.debug('Sender {} is using tcp://{}:{}'.
                         format(name, self.ip, port))
            self.socket.connect('tcp://{}:{}'.format(self.ip, port))
            
        if not self.__ping__():
            msg = ('Receiver ipc://{self.name} is not answering'
                   if self.local else 
                   ('Receiver tcp://{self.ip}:{port} (name "{self.name}"") '
                    'is not answering'))
            raise PipeException(msg.format(self=self, port=port))

    def set_debug_name(self):
        """Name for debugging purposes.

        Try to find the name of the object from where we are using the
        sender.

        """
        try:
            self.my_actor = inspect.stack()[3][0].f_locals['self'].__class__
        except:
            self.my_actor = '%s-%s-%s' %tuple(inspect.stack()[3][1:4])

    def _reply_to_address(self, recv_socket):
        """Create a temporary socket to listen for replies."""
        if self.local:
            addr = 'ipc://__{}__'.format(self.name)
            recv_socket.bind(addr)
            return addr
        else:
            ip = get_local_ip(self.ip)
            port = recv_socket.bind_to_random_port('tcp://*')
            return 'tcp://{}:{}'.format(ip, port)
            
    def __ping__(self):
        """Low level ping.

        Check the reader loop of the receiver is running.

        """
        with zmq_socket(zmq.PULL) as r:
            _reply_to = self._reply_to_address(r)
            logger.debug('reply_to = {}'.format(_reply_to))
            self.socket.send_json({'__tag__': '__low_level_ping__',
                                   'reply_to': _reply_to})
            try:
                r.set(zmq.RCVTIMEO, 1000)
                resp = r.recv_json()
                return resp['__tag__'] == '__pong__'
            except zmq.Again:
                return False
            
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def write(self, data):
        logger.debug('Send from %s to %s' %(self.my_actor, self.name))
        logger.debug('  message: %s' %(data,))
        self.socket.send_json(data)

    def close(self):
        self.socket.close()

    def close_receiver(self, confirm_to=None, confirm_msg=None):
        self.put({'__tag__': '__quit__',
                  'confirm_to': confirm_to,
                  'confirm_msg': confirm_msg})
        
    # synonym
    put = write

    def __del__(self):
        self.close()

        
class Server(object):
    """A generic REQ/REP server."""
    
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.setup()

    def setup(self):
        pass
        
    @property
    def name(self):
        return '{}-{}:{}'.format(type(self).__name__,
                                 self.ip,
                                 self.port)
        
    def start(self):
        self.thread = threading.Thread(target=self._server,
                                       args=(logger,))
        self.thread.name = self.name
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        with zmq_socket(zmq.REQ) as s:
            ip = self.ip if self.ip != '*' else 'localhost'
            s.connect('tcp://{}:{}'.format(ip, self.port))
            s.send_json({'__quit__': True})
            s.recv_json()
            self.thread.join()
            
    def _server(self, logger):
        with zmq_socket(zmq.REP) as s:
            s.bind('tcp://{}:{}'.format(self.ip, self.port))
            while True:
                data = s.recv_json()
                resp = None
                try:
                    if data.get('__quit__'):
                        logger.debug('asked to shutdown')
                        return
                    resp = self.handle(data)
                except Exception:
                    exc = traceback.format_exc()
                    logger.debug('got an exception:')
                    logger.debug(exc)
                    resp = {'exception': exc}
                finally:
                    s.send_json(resp)


class NameBroker(Server):
    """A namebroker server.

    Register, unregister, and provide ports associated to names.

    """

    PORT = 5555

    def __init__(self):
        super(NameBroker, self).__init__('*', self.PORT)
        
    def setup(self):
        self.names = {}

    def handle(self, data):
        cmd = data['__tag__']
        try:
            return getattr(self, cmd)(data)
        except AttributeError:
            return None

    def get(self, data):
        name = data['__name__']
        port = self.names.get(name, None)
        return {'__port__': port}
        
    def register(self, data):
        name = data['__name__']
        port = data['__port__']
        self.names[name] = port

    def unregister(self, data):
        name = data['__name__']
        try:
            del self.names[name]
        except KeyError:
            pass

    def list(self, data):
        if self.names:
            col = max(map(len, self.names))
            for name in self.names:
                logger.debug('{{:>{}}}: {{}}'
                             .format(col)
                             .format(name, self.names[name]))
        else:
            logger.debug('No registered names')            