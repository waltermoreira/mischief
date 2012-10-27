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
from het2_common import log
from het2_common.globals import DEPLOY_PATH

logger = log.setup('pipe', 'to_file')

Context = zmq.Context()

# Set some time to wait for sockets to deliver messages
# We don't want infinite time, since it may block when exiting
# the application
Context.linger = 5000 # ms

def path_to(name):
    """
    Path for unix socket with name ``name``.
    """
    return os.path.join(DEPLOY_PATH, 'lib/run/actor_pipes', name)

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
        self.name = name
        self.path = path_to(self.name)
        self.socket = Context.socket(zmq.PUSH)
        self.socket.connect('ipc://%s' %self.path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def write(self, data):
        logger.debug('Send to %s' %self.name)
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
            queue.put(data)
    except Exception:
        import traceback
        logger.debug('Reader thread for %s got an exception:' %(socket_name,))
        logger.debug(traceback.format_exc())
            
        
