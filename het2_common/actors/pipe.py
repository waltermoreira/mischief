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

logger = log.setup('pipe', 'to_console')

Context = zmq.Context()

# Set some time to wait for sockets to deliver messages
# We don't want infinite time, since it may block when exiting
# the application
Context.linger = 5000 # ms

class PipeReadTimeout(Exception):
    pass

class Pipe(object):

    def __init__(self, name, mode='r'):
        self.name = name
        self.reader_queue = None
        self.reader_thread = None
        self.path = self._path_to(name)
        self.mode = mode
        if mode == 'r':
            self._open_read_pipe()
        elif mode == 'w':
            self._open_write_pipe()
        else:
            raise Exception("unknown mode. Should be 'r' or 'w'")

    def qsize(self):
        return self.reader_queue.qsize()
        
    def close(self, confirm_to=None, confirm_msg=None):
        """
        Close this end of the pipe.

        If ``confirm_to`` is not None, then ``confirm_msg`` is sent to
        the pipe Pipe(confirm_to).
        """
        if self.reader_thread:
            print 'will send quit to thread'
            # force reader thread to finish
            self.push_socket = Context.socket(zmq.PUSH)
            self.push_socket.connect('ipc://%s' %self.path)
            # send non-json data
            self.push_socket.send('__quit')
            self.push_socket.close()
            print 'sent, will join'
            self.reader_thread.join()
            print 'joined'
        print 'will close socket'
        self.socket.close()
        print 'closed'
        if self.mode == 'r':
            print 'will unlink'
            try:
                os.unlink(self.path)
            except OSError:
                pass
            print 'unlinked'
        if confirm_to is not None:
            confirm_pipe = Pipe(confirm_to, 'w')
            confirm_pipe.put(confirm_msg)
            confirm_pipe.close()

    def write(self, data):
        self.socket.send_json(data)

    # synonym
    put = write
    
    def read(self, block=True, timeout=None):
        x = self.reader_queue.get(block, timeout)
        logger.debug('reader queue for %s got %s' %(self.name, x))
        return x
            
    # synonym
    get = read
    
    def _open_write_pipe(self):
        self.socket = Context.socket(zmq.PUSH)
        self.socket.connect('ipc://%s' %self.path)

    def _open_read_pipe(self):
        self.socket = Context.socket(zmq.PULL)
        self.socket.bind('ipc://%s' %self.path)
        
        self.reader_queue = Queue()
        self.reader_thread = threading.Thread(target=_reader,
                                              args=(self.socket, self.reader_queue, self.name))
        self.reader_thread.name = 'reader-%s'%(self.name,)
        self.reader_thread.daemon = True
        self.reader_thread.start()
        
    def _path_to(self, name):
        return os.path.join(DEPLOY_PATH, 'lib/run/actor_pipes', name)

        
def _reader(socket, queue, name):
    """
    Thread function that reads the zmq socket and puts the data
    into the queue
    """

    try:
        while True:
            data = socket.recv_json()

            logger.debug('reading from pipe %s into python queue' %(name,))
            logger.debug('  before inserting: size(queue) = %s' %(queue.qsize()))

            queue.put(data)

            logger.debug('  after inserting: size(queue) = %s' %(queue.qsize()))
    except ValueError:
        # non-json data on the socket means we want to stop the
        # thread
        return
            
        
def grouper(n, iterable):
    """
    grouper(3, 'ABCDEFG') --> ABC DEF Gxx
    """
    args = [iter(iterable)] * n
    return imap(lambda x: ''.join(x),
                izip_longest(fillvalue=' ', *args))

def write(p, c):
    p.write(c*100000)
    print 'ended writing', c
    
def test():
    p1 = Pipe('foo', 'r')

    symbols = ['X', '_', 'R', '$']
    threads = []
    pipes = []
    for c in symbols:
        p = Pipe('foo', 'w')
        pipes.append(p)
        threads.append(threading.Thread(target=write, args=(p, c)))
    for t in threads:
        t.start()
    return p1

