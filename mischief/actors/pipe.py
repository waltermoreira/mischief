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
    ipaddr = ''
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((target, 8000))
        ipaddr = s.getsockname()[0]
    finally:
        s.close()
    return ipaddr 
    

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

    def send_to_local_namebroker(self, msg):
        socket = Context.socket(zmq.PUSH)
        socket.connect('tcp://localhost:{}'.format(NameBroker.PORT))
        socket.send_json(msg)

    def _reader(self, logger):
        """
        Thread function that reads the zmq socket and puts the data
        into the queue
        """
        import collections

        socket_name = self.path
        queue = self.reader_queue
        
        socket = Context.socket(zmq.PULL)
        if os.name == 'posix':
            socket.bind('ipc://%s' %socket_name)
        port = socket.bind_to_random_port('tcp://*')
        self.send_to_local_namebroker({
            '__tag__': 'register',
            '__name__': self.name,
            '__port__': port})

        try:
            while True:
                data = socket.recv_json()
                if data['tag'] == '__quit__':
                    # means to shutdown the thread
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
                if data['tag'] == '__tcp_ping__':
                    # respond to a particular port
                    reply_to_port = data['reply_to_port']
                    reply_to_ip = data['reply_to_ip']
                    reply_socket = Context.socket(zmq.PUSH)
                    reply_socket.connect('tcp://{}:{}'.format(reply_to_ip,
                                                              reply_to_port))
                    reply_socket.send_json({'tag': '__pong__'})
                    reply_socket.close()
                    continue
                queue.put(data)
        except Exception:
            import traceback
            logger.debug('Reader thread for {} got an exception:'
                         .format(socket_name))
            logger.debug(traceback.format_exc())
        
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

def get_port_for(name, at):
    my_ip = get_local_ip(at)
    reply_socket = Context.socket(zmq.PULL)
    my_port = reply_socket.bind_to_random_port('tcp://*')

    socket = Context.socket(zmq.PUSH)
    socket.connect('tcp://{}:{}'.format(at, NameBroker.PORT))
    socket.send_json(
        {'__tag__': 'get',
         '__name__': name,
         '__reply_to__': 'tcp://{}:{}'.format(my_ip, my_port)})
    socket.close()

    reply_socket.set(zmq.RCVTIMEO, 1000)
    try:
        resp = reply_socket.recv_json()
        return resp['__port__']
    finally:
        reply_socket.close()


        
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
        socket = Context.socket(zmq.PUSH)
        ip = self.ip if self.ip != '*' else 'localhost'
        socket.connect('tcp://{}:{}'.format(ip, self.port))
        socket.send_json({'__quit__': True})
        self.thread.join()

    def _server(self, logger):
        socket = Context.socket(zmq.PULL)
        socket.bind('tcp://{}:{}'.format(self.ip, self.port))

        try:
            while True:
                data = socket.recv_json()
                if data.get('__quit__'):
                    logger.debug('asked to shutdown')
                    return
                self.handle(data)
        except Exception:
            import traceback
            logger.debug('got an exception:')
            logger.debug(traceback.format_exc())
        

class ExternalListener(Server):

    def handle(self, data):
        name = data['__to__']
        logger.debug('Got data in external directed to {}'.format(name))
        with Sender(name) as s:
            s.put(data)

            
class NameBroker(Server):

    PORT = 5555

    def __init__(self):
        super(NameBroker, self).__init__('*', self.PORT)
        
    def setup(self):
        self.names = {}

    def handle(self, data):
        cmd = data['__tag__']
        try:
            getattr(self, cmd)(data)
        except AttributeError:
            pass

    def get(self, data):
        name = data['__name__']
        reply_to = data['__reply_to__']
        port = self.names.get(name, None)
        try:
            reply_socket = Context.socket(zmq.PUSH)
            reply_socket.connect(reply_to)
            reply_socket.send_json({'__port__': port})
        finally:
            reply_socket.close()
        
        
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
        try:
            col = max(map(len, self.names))
        except ValueError:
            logger.debug('No registered names')
            return
        for name in self.names:
            logger.debug('{{:>{}}}: {{}}'
                         .format(col)
                         .format(name, self.names[name]))
