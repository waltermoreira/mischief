import errno
import os
import threading
import traceback
import socket
import inspect
from six.moves import queue


Queue = queue.Queue
Empty = queue.Empty


import zmq
from .namebroker import NameBrokerClient
from ..log import setup, show_msg
from ..zmq_tools import zmq_socket, Context
from ..exceptions import PipeException, PipeEmpty


logger = setup(to=['file'])

MIN_PORT = 50000
MAX_PORT = 60000

ACTORS_DIRECTORY = '/tmp/actors_{}'.format(os.environ.get('USER', 'NO_USER'))
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
Context.linger = 5000  # ms


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
        # 8000 is just a dummy number, it doesn't actually connect to
        # the port
        s.connect((target, 8000))
        s.send(b'1')
        ipaddr = s.getsockname()[0]
    except socket.error:
        pass
    finally:
        s.close()
    return ipaddr


def is_local_ip(target):
    """Check that ``target`` is a local ip."""
    if target in (None, 'localhost', '127.0.0.1'):
        return True
    return target == get_local_ip(target)


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

        {'tag': '__ping__',
         'reply_to': sender_address}

    If the reader loop is running, it will respond with the message

        {'tag': '__pong__'}

    Receiver requires the dependencies: NameBrokerClient and Sender.

    """
    def __init__(self, name, ip='localhost', use_remote=True,
                 ignore_namebroker=True):
        self.name = name
        self.ip = ip
        self.use_remote = use_remote
        self.ignore_namebroker = ignore_namebroker

        self.path = path_to(name)

        self.namebroker_client = NameBrokerClient(at=self.ip)

        self.reader_queue = Queue()
        socket = self.setup_reader()
        self.reader_thread = threading.Thread(target=self._reader,
                                              args=(logger, socket))
        self.reader_thread.name = 'receiver-{}'.format(self.name)
        self.reader_thread.daemon = True
        self.reader_thread.start()
        logger.debug('Receiver {} created'.format(self.name))

    def address(self):
        return self.name, self.ip, self.port

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        self.reader_thread.join()

    def _reader(self, logger, socket):
        try:
            self._reader_loop(socket)
            logger.debug('  ..._reader_loop exited')
        finally:
            socket.close()
            logger.debug('  ...closed Sender socket after reader loop exit')

    def _reader_loop(self, socket):
        """
        Thread function that reads the zmq socket and puts the data
        into the queue
        """
        queue = self.reader_queue
        while True:
            try:
                data = socket.recv_json()
                __tag__ = data.get('tag')
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
                    self.namebroker_client.unregister(self.name)
                    return
                if __tag__ == '__ping__':
                    # answer special message without going to the receive,
                    # since the actor may be doing something long lasting
                    # and not reading the queue
                    with Sender(data['reply_to']) as sender:
                        sender.put({'tag': '__pong__'})
                    # avoid inserting this message in the queue
                    continue
                if __tag__ == '__address__':
                    # Fill the port info for my address
                    with Sender(data['reply_to']) as sender:
                        sender.put({'tag': 'reply',
                                    'address': self.address(),
                                    'pid': os.getpid()})
                    continue
                if __tag__ == '__low_level_ping__':
                    # answer a ping from a straight zmq socket
                    sender = data['reply_to']
                    with zmq_socket(zmq.PUSH) as s:
                        s.connect(sender)
                        s.send_json({'tag': '__pong__'})
                    continue
            except Exception:
                exc = traceback.format_exc()
                logger.debug('Reader thread for {} got an exception:'
                             .format(self.path))
                logger.debug(exc)
                return
            queue.put(data)

    def setup_reader(self):
        """Create the socket for the reader and bind it."""
        s = Context.socket(zmq.PULL)
        if os.name == 'posix':
            s.bind('ipc://{}'.format(self.path))
        if self.use_remote or os.name != 'posix':
            self.port = s.bind_to_random_port(
                'tcp://*', min_port=MIN_PORT, max_port=MAX_PORT)
            try:
                self.namebroker_client.register(self.name, self.port)
            except PipeException:
                if not self.ignore_namebroker:
                    raise
        else:
            self.port = None
        return s

    def qsize(self):
        return self.reader_queue.qsize()

    def read(self, block=True, timeout=None):
        try:
            x = self.reader_queue.get(block, timeout)
            # logger.debug('Receive at %s' %(self.name,))
            # logger.debug('  message: {}'.format(str(x)))
            return x
        except Empty:
            raise PipeEmpty()

    def close(self, confirm_to=None, confirm_msg=None):
        with Sender(self.address()) as sender:
            sender.close_receiver(confirm_to, confirm_msg)
        logger.debug('Receiver {} destroyed'.format(self.name))

    # synonym
    get = read


def get_port_for(name, at):
    """Consult namebroker for the port associated to a name."""
    resp = NameBrokerClient.send(at, {'__tag__': 'get',
                                      '__name__': name})
    return resp['__port__']


class Sender(object):
    """The sender end of a pipe.

    Usually, the address for the sender is returned by the
    ``address_relative_to`` method of a receiver.

    The address has the form::

        (name, ip, port)

    A sender tries to use a local socket (ipc) if possible, unless the
    argument ``use_local`` is set to ``False``. In non-posix systems,
    it always use tcp.

    Use as::

        with Sender(address) as s:
            ...
            s.put(msg)
            ...

    If the port is not known, get it from the ``NameBroker``
    objects.

    """

    def __init__(self, address, use_local=True):
        self.set_debug_name()
        self.name, self.ip, self.port = self.address = address
        self.local = (use_local and is_local_ip(self.ip)
                      if os.name == 'posix' else False)
        self.socket = Context.socket(zmq.PUSH)

        if self.local:
            self.path = path_to(self.name)
            logger.debug('  ...sender {} is using ipc'.format(self.name))
            self.socket.connect('ipc://{}'.format(self.path))
        else:
            logger.debug('  ...sender {self.name} is using '
                         'tcp://{self.ip}:{self.port}'.format(self=self))
            self.socket.connect('tcp://{self.ip}:{self.port}'
                                .format(self=self))

        if not self.__ping__():
            msg = ('Receiver ipc://{self.name} is not answering'
                   if self.local else
                   ('Receiver tcp://{self.ip}:{self.port} '
                    '(name "{self.name}") is not answering'))
            raise PipeException(msg.format(self=self))
        logger.debug('Sender {} created (in {})'
                     .format(self.name, self.my_actor))

    def set_debug_name(self):
        """Name for debugging purposes.

        Try to find the name of the object from where we are using the
        sender.

        """
        try:
            self.my_actor = (inspect.stack()[3][0].f_locals['self']
                             .__class__.__name__)
        except:
            self.my_actor = '{}-{}-{}'.format(*inspect.stack()[3][1:4])

    def _temp_receiver(self, recv_socket):
        """Create a temporary socket to listen for replies."""
        if self.local:
            addr = 'ipc://{}'.format(path_to('__{}__'.format(self.name)))
            recv_socket.bind(addr)
            return addr
        else:
            port = recv_socket.bind_to_random_port(
                'tcp://*', min_port=MIN_PORT, max_port=MAX_PORT)
            ip = get_local_ip(self.ip)
            return 'tcp://{}:{}'.format(ip, port)

    def __ping__(self):
        """Low level ping.

        Check the reader loop of the receiver is running.

        """
        with zmq_socket(zmq.PULL) as r:
            address = self._temp_receiver(r)
            self.socket.send_json({'tag': '__low_level_ping__',
                                   'reply_to': address})
            try:
                r.set(zmq.RCVTIMEO, 1000)
                resp = r.recv_json()
                return resp['tag'] == '__pong__'
            except zmq.Again:
                return False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def write(self, data):
        logger.debug('From {} to {}:\n{}'.
                     format(self.my_actor, self.name,
                            show_msg(data, indent=4)))
        self.socket.send_json(data)

    def close(self):
        self.socket.close()
        logger.debug('Sender {} destroyed'.format(self.name))

    def close_receiver(self, confirm_to=None, confirm_msg=None):
        self.put({'tag': '__quit__',
                  'confirm_to': confirm_to,
                  'confirm_msg': confirm_msg})

    # synonym
    put = write

    def __del__(self):
        self.close()
