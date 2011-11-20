from itertools import izip_longest, imap
import time
import struct
import select
import uuid
import json
import errno
import os
import threading

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

def grouper(n, iterable):
    "grouper(3, 'ABCDEFG') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return imap(lambda x: ''.join(x),
                izip_longest(fillvalue=' ', *args))

class Pipe(object):

    # Leave room to add identifier and part number
    MAX_BUFSIZE = select.PIPE_BUF - 50
    # substract 1 to account for the additional \n
    MAX_UNSPLIT = select.PIPE_BUF - 1

    STRUCT = 'c32si{}s'.format(MAX_BUFSIZE)
    
    def __init__(self, name, mode='r'):
        self.parts = {}
        self.path = self._path_to(name)
        if not os.path.exists(self.path):
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
        Close this end of the pipe. The same object can be used
        again, for reading or writing.
        """
        self.fd.close()
        self._aux_fd.close()

    def destroy(self):
        """
        Destroy the pipe, by removing the pipe.
        """
        self.close()
        os.unlink(self.path)

    def _pack(self, ident, n_part, s):
        return struct.pack(self.STRUCT,
                           '}', ident.hex, n_part, s)

    def _unpack(self, s):
        return struct.unpack(self.STRUCT, s)
                             
    def _split_write(self, s):
        ident = uuid.uuid1()
        for i, part in enumerate(grouper(self.MAX_BUFSIZE, s)):
            print 'writing part', i
            self.fd.write(self._pack(ident, i, part))
        # write part number -1 to signal end of parts
        self.fd.write(self._pack(ident, -1, ' '*self.MAX_BUFSIZE))
                           
    def write(self, data):
        if self.mode == 'r':
            raise Exception('pipe already open for reading')
        buf = json.dumps(data)
        if len(buf) < self.MAX_UNSPLIT:
            self.fd.write(buf+'\n')
        else:
            self._split_write(buf)

    def _split_read(self, s):
        _, ident, part, data = self._unpack(s)
        if part < 0:
            print 'collecting parts'
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
        print 'saved part'
        return None
        
    def _read(self):
        if self.mode == 'w':
            raise Exception('pipe already open for writing')
        try:
            first_char = self.fd.read(1)
            if first_char == '}':
                print 'multipart message'
                # cannot be a JSON, so we are reading split big chunk of data
                data = first_char + self.fd.read(struct.calcsize(self.STRUCT)-1)
                return self._split_read(data)
            else:
                print 'json data'
                data = first_char + self.fd.readline()
                return json.loads(data)
        except IOError as err:
            if err.errno == errno.EWOULDBLOCK:
                # No data from clients
                return None
            raise
        except ValueError:
            traceback.print_exc()
            # Got wrong data, or EOF
            return None

    def read(self, block=False):
        if block:
            while True:
                data = self._read()
                if data is not None:
                    return data
                time.sleep(0.01)
        else:
            return self._read()
                
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

    def _path_to(self, name):
        return os.path.join('/tmp/actor_pipes', name)
    
    
