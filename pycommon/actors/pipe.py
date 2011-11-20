from itertools import izip_longest, imap
from gevent import sleep
from Queue import Queue
import time
import struct
import select
import uuid
import json
import errno
import os
import threading
import traceback

class PipeReadTimeout(Exception):
    pass

class Pipe(object):

    # Maximum size allowed for the json string representing the data,
    # before splitting it in a multi-part message.
    #
    # By default, we want it to fit in PIPE_BUF bytes, as the write
    # operation is guarantee to be atomic (substract 1 to account for
    # the additional \n)
    MAX_UNSPLIT = select.PIPE_BUF - 1

    # Size of chunks into which we split the json data when it is
    # bigger than PIPE_BUF.
    #
    # Leave room to add identifier and part number
    MAX_BUFSIZE = select.PIPE_BUF - 50

    # Format to serialize the chunks of json data.
    #
    # '}' + 32 char ident + json_chunk
    #
    # The initial '}' is so we can tell a split message from a json
    # message, since a string starting with '}' is not a valid json
    # object.
    STRUCT = 'c32si{}s'.format(MAX_BUFSIZE)
    
    def __init__(self, name, mode='r', create=False):
        self.parts = {}
        self.name = name
        self.path = self._path_to(name)
        if create and not os.path.exists(self.path):
            print 'mkfifo', self.path
            os.mkfifo(self.path)
        self.mode = mode
        if mode == 'r':
            self._open_read_pipe()
        elif mode == 'w':
            self._open_write_pipe()
        else:
            raise Exception("unknown mode. Should be 'r' or 'w'")

    def close(self):
        """
        Close this end of the pipe.
        """
        self.fd.close()
        self._aux_fd.close()
        # destroy queue, in case it is a reader, so attempts to read
        # do not find an empty queue
        self.reader_queue = None

    def destroy(self):
        """
        Destroy the pipe, by removing the pipe.
        """
        self.close()
        print 'unlinking', self.path
        os.unlink(self.path)

    def _pack(self, ident, n_part, s):
        return struct.pack(self.STRUCT,
                           '}', ident.hex, n_part, s)

    def _unpack(self, s):
        return struct.unpack(self.STRUCT, s)
                             
    def _split_write(self, s):
        """
        Split the string 's' into parts that fit in PIPE_BUF, and
        write them to the pipe.

        Careful: if the string 's' is bigger than the full pipe
        buffers (usually 64k), this write will block until there is
        space in the pipe to write all the remaining parts.
        """
        ident = uuid.uuid1()
        for i, part in enumerate(grouper(self.MAX_BUFSIZE, s)):
            # write a part
            self.fd.write(self._pack(ident, i, part))
        # write part number -1 to signal end of parts
        self.fd.write(self._pack(ident, -1, ' '*self.MAX_BUFSIZE))
                           
    def write(self, data):
        if self.mode == 'r':
            raise Exception('pipe already open for reading')
        buf = json.dumps(data)
        if len(buf) < self.MAX_UNSPLIT:
            # if the json string fits in PIPE_BUF bytes, just write it
            # with a newline, so it can be read with 'readline'
            self.fd.write(buf+'\n')
        else:
            # if the json string is too big, split it
            self._split_write(buf)

    def _split_read(self, s):
        _, ident, part, data = self._unpack(s)
        if part < 0:
            # we have all the data, concatenate it in the proper order
            parts = self.parts[ident]
            result = [parts[k] for k in range(len(parts))]
            # forget all parts for this client
            del self.parts[ident]
            # return the JSON
            return json.loads(''.join(result))
        # save the data with the proper part number
        parts = self.parts.setdefault(ident, {})
        parts[part] = data
        # Return as if there were no data in the pipe, until all parts
        # are read and concatenated
        return None
        
    def _read(self):
        if self.mode == 'w':
            raise Exception('pipe already open for writing')
        try:
            first_char = self.fd.read(1)
            if first_char == '}':
                # cannot be a JSON, so we are reading a split big
                # chunk of data
                data = first_char + self.fd.read(struct.calcsize(self.STRUCT)-1)
                return self._split_read(data)
            else:
                # read a full line, and decode it as json
                data = first_char + self.fd.readline()
                return json.loads(data)
        except IOError as err:
            if err.errno == errno.EWOULDBLOCK:
                # No data from clients
                return None
            # do not ignore other kind of ioerrors
            raise

    def _read_full(self):
        # Try to read data until we get an object. If there is
        # data and it is a simple json object, it will return
        # immediatly. Otherwise, it will block and keep waiting or
        # reading parts of a multi-part message until it is
        # complete
        while True:
            data = self._read()
            if data is not None:
                return data
            time.sleep(0.01)

    def read(self, block=True, timeout=None):
        try:
            return self.reader_queue.get(block, timeout)
        except AttributeError:
            raise Exception('pipe closed')
        
    def _open_write_pipe(self):
        # Open secondary readonly fd so client doesn't get an error if
        # trying to write before a listener is up
        self._aux_fd = os.fdopen(
            os.open(self.path, os.O_RDONLY | os.O_NONBLOCK), 'r', 0)
        self.fd = os.fdopen(
            os.open(self.path, os.O_WRONLY), 'w', 0)

    def _open_read_pipe(self):
        self.fd = os.fdopen(
            os.open(self.path, os.O_RDONLY | os.O_NONBLOCK), 'r', 0)
        # Open secondary writeonly fd so we don't get EOF if all
        # clients disconnect
        self._aux_fd = os.fdopen(
            os.open(self.path, os.O_WRONLY | os.O_NONBLOCK), 'w', 0)
        self.reader_thread = threading.Thread(target=self._reader)
        self.reader_thread.name = 'reader-%s'%(self.name,)
        self.reader_thread.start()
        
    def _reader(self):
        self.reader_queue = Queue()
        try:
            while True:
                data = self._read_full()
                if data == '__quit':
                    return
                self.reader_queue.put(data)
        except ValueError:
            # file descriptor closed, just exit
            return
            
    def _path_to(self, name):
        return os.path.join('/tmp/actor_pipes', name)

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

