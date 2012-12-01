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
import inspect
from .. import log

logger = log.setup('pipe', 'to_file')

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

class Receiver(object):

    def __init__(self, name):
        self.name = name
        self.path = path_to(name)

        self.reader_queue = Queue()
        self.reader_thread = threading.Thread(target=_reader,
                                              args=(self.path, self.reader_queue, logger))
        self.reader_thread.name = 'reader-%s'%(self.name,)
        self.reader_thread.daemon = True
        self.reader_thread.start()

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
    
class Sender(object):

    def __init__(self, name):
        try:
            self.my_actor = inspect.stack()[2][0].f_locals['self'].__class__
        except:
            self.my_actor = '%s-%s-%s' %tuple(inspect.stack()[2][1:4])
        
        self.name = name
        self.path = path_to(self.name)
        self.socket = Context.socket(zmq.PUSH)
        self.socket.connect('ipc://%s' %self.path)

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
        self.put({'tag': '__quit__',
                  'confirm_to': confirm_to,
                  'confirm_msg': confirm_msg})
        
    # synonym
    put = write

    def __del__(self):
        self.close()
    
def _reader(socket_name, queue, logger):
    """
    Thread function that reads the zmq socket and puts the data
    into the queue
    """
    import collections
    
    socket = Context.socket(zmq.PULL)
    socket.bind('ipc://%s' %socket_name)

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
            queue.put(data)
    except Exception:
        import traceback
        logger.debug('Reader thread for %s got an exception:' %(socket_name,))
        logger.debug(traceback.format_exc())
            
        
