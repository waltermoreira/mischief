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
    """
    Path for unix socket with name ``name``.
    """
    return os.path.join(ACTORS_DIRECTORY, name)

def get_local_ip(target):
    """Get the *local* ip.

    Warning: it doesn't mean the ``target`` would be able to connect
    to the ``local`` ip (for example, if the ``local`` machine is
    behind a NAT).

    """
    ipaddr = ''
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((target, 8000)) # 8000 is just a dummy number, it
                                  # doesn't actually connect to the
                                  # port
        ipaddr = s.getsockname()[0]
    finally:
        s.close()
    return ipaddr 

@contextmanager
def socket(zmq_type):
    s = Context.socket(zmq_type)
    yield s
    s.close()
    
class Receiver(object):

    def __init__(self, name):
        self.name = name
        self.path = path_to(name)

        self.reader_queue = Queue()
        self.reader_thread = threading.Thread(target=self._reader,
                                              args=(logger,))
        self.reader_thread.name = 'reader-%s'%(self.name,)
        self.reader_thread.daemon = True
        self.reader_thread.start()

    def _reader_loop(self, socket):
        queue = self.reader_queue
        while True:
            data = socket.recv_json()
            try:
                if data['tag'] == '__quit__':
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
                if data['tag'] == '__ping__':
                    # answer special message without going to the receive,
                    # since the actor may be doing something long lasting
                    # and not reading the queue
                    with Sender(data['reply_to']) as sender:
                        sender.put({'tag': '__pong__'})
                    # avoid inserting this message in the queue
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
        with socket(zmq.PULL) as s:
            if os.name == 'posix':
                s.bind('ipc://%s' %self.path)
            port = s.bind_to_random_port('tcp://*')
            self.send_to_local_namebroker({
                '__tag__': 'register',
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
        
    # synonym
    get = read

    def __del__(self):
        self.close()


def send_to_local_namebroker(msg, timeout=1000):
    with socket(zmq.REQ) as s:
        s.set(zmq.RCVTIMEO, timeout)
        s.connect('tcp://localhost:{}'.format(NameBroker.PORT))
        s.send_json(msg)
        return s.recv_json()
        
def get_port_for(name, at):
    resp = send_to_local_namebroker({'__tag__': 'get',
                                     '__name__': name})
    return resp['__port__']

        
class Sender(object):
    """
    A sender pipe.

    ``name`` can be

    - ``identifier``: a local pipe
    - ``ip:identifier``: a remote pipe
    """

    def __init__(self, name):
        try:
            self.my_actor = inspect.stack()[2][0].f_locals['self'].__class__
        except:
            self.my_actor = '%s-%s-%s' %tuple(inspect.stack()[2][1:4])

        self.ip, self.name = name.split(':') if ':' in name else (None, name)
        if os.name != 'posix' and self.ip is None:
            self.ip = 'localhost'
        self.path = path_to(self.name)
        self.socket = Context.socket(zmq.PUSH)
        if self.ip is not None:
            port = get_port_for(self.name, at=self.ip)
            self.socket.connect('tcp://{}:{}'.format(self.ip, port))
        else:
            self.socket.connect('ipc://%s' %self.path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def write(self, data):
        logger.debug('Send from %s to %s' %(self.my_actor, self.name))
        logger.debug('  message: %s' %(data,))
        if self.ip is not None:
            data['__to__'] = self.name
        self.socket.send_json(data)

    def close(self):
        self.socket.close()

    def close_receiver(self, confirm_to=None, confirm_msg=None):
        self.put({'tag': '__quit__',
                  'confirm_to': confirm_to,
                  'confirm_msg': confirm_msg})
        
    # synonym
    put = write

    def __del__(self):
        self.close()

        
class Server(object):
    
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
        with socket(zmq.REQ) as s:
            ip = self.ip if self.ip != '*' else 'localhost'
            s.connect('tcp://{}:{}'.format(ip, self.port))
            s.send_json({'__quit__': True})
            s.recv_json()
            self.thread.join()
            
    def _server(self, logger):
        with socket(zmq.REP) as s:
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