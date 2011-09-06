from gevent import spawn, socket
from gevent.server import StreamServer
from gevent.queue import Queue, Empty
from gevent.coros import RLock
import multiprocessing
import json
import socket as py_socket
import time
import errno

IP = 'localhost'
PORT = 5123

class Manager(object):

    def __init__(self, address=(IP, PORT)):
        self.server = StreamServer(address, self.handle_request)
        self.queues = {}
        self.conns = 0
        self.connections = {}
        self.address = address

    def report(self):
        print '---'
        for q in self.queues:
            print 'Queue:', q
            queue = self.queues[q]
            n = queue.qsize()
            print 'Size:', n
            for i in range(n):
                x = queue.get()
                print ' elt:', x
                queue.put(x)
                
    def _is_alive(self):
        try:
            s = py_socket.create_connection(self.address)
            s.close()
            return True
        except py_socket.error:
            return False
        
    def start(self):
        if self._is_alive():
            return
        server_proc = multiprocessing.Process(target=self.server.serve_forever)
        server_proc.daemon = True
        server_proc.start()
        while not self._is_alive():
            time.sleep(0.1)
        return server_proc

    def stop(self):
        s = py_socket.create_connection(self.address)
        f = s.makefile('w', bufsize=0)
        f.write(json.dumps({'cmd': 'stop_server'}) + '\n')
        f.close()
        
    def handle_request(self, sock, address):
        self.conns += 1
        stream = sock.makefile('w', bufsize=0)
        try:
            while True:
                try:
                    obj = json.loads(stream.readline())
                except socket.error as exc:
                    # If we get a 'connection reset by peer' it means
                    # the actor is finishing while doing some
                    # request. We ignore it.
                    if exc.errno == errno.ECONNRESET:
                        import traceback
                        print '-- Connection reset by peer. Ignoring...'
                        print '\n'.join('-- '+x for x in traceback.format_exc())
                        return
                except ValueError as exc:
                    try:
                        stream.write(json.dumps({'status': False,
                                                 'type': 'not_a_json',
                                                 'msg': exc.message}) + '\n')
                    except:
                        pass
                    return
                cmd = obj['cmd']
                if cmd == 'put':
                    self.put(obj['name'], obj['arg'])
                elif cmd == 'get':
                    timeout = obj.get('timeout', None)
                    res = self.get(obj['name'], timeout=timeout)
                    stream.write(json.dumps(res) + '\n')
                elif cmd == 'quit':
                    return
                elif cmd == 'del':
                    try:
                        self.queues[obj['name']].put(None)
                        del self.queues[obj['name']]
                    except KeyError:
                        pass
                elif cmd == 'touch':
                    self.touch(obj['name'])
                elif cmd == 'stats':
                    self.stats()
                elif cmd == 'size':
                    res = self.size(obj['name'])
                    stream.write(json.dumps(res) + '\n')
                elif cmd == 'flush':
                    self.flush(obj['name'])
                    stream.write(json.dumps({'status': True}) + '\n')
                elif cmd == 'stop_server':
                    self.server.stop()
                    return
                elif cmd == 'report':
                    self.report()
                else:
                    stream.write(json.dumps({'status': False,
                                             'type': 'unknown_cmd'}) + '\n')
        finally:
            self.conns -= 1

    def flush(self, name):
        self.queues[name] = Queue()
        
    def get(self, name, timeout=None):
        try:
            q = self.queues[name]
            result = q.get(timeout=timeout)
            return {'status': True,
                    'result': result}
        except Empty:
            return {'status': False,
                    'type': 'empty'}
        except KeyError:
            return {'status': False,
                    'type': 'not_found',
                    'msg': "queue '%s' not found" %name}

    def touch(self, name):
        self.queues.setdefault(name, Queue())
        
    def put(self, name, obj):
        q = self.queues[name]
        q.put(obj)

    def size(self, name):
        q = self.queues[name]
        return {'status': True,
                'result': q.qsize()}
        
    def stats(self):
        print 'num queues', len(self.queues)
        print 'num connections', self.conns
        print 'queues:', self.queues.keys()
        
class QueueError(Exception):
    pass

class QueueRef(object):

    def __init__(self, name, address=(IP, PORT)):
        self.name = name
        s = py_socket.create_connection(address)
        self.sock = s.makefile('w', bufsize=0)
        self.sock.write(json.dumps({'cmd': 'touch',
                                    'name': self.name}) + '\n')
        
    def put(self, obj):
        self.sock.write(json.dumps({'cmd': 'put',
                                    'name': self.name,
                                    'arg': obj}) + '\n')

    def get(self, timeout=None):
        self.sock.write(json.dumps({'cmd': 'get',
                                    'timeout': timeout,
                                    'name': self.name}) + '\n')
        ret = json.loads(self.sock.readline())
        if ret['status']:
            return ret['result']
        if ret['type'] == 'empty':
            raise Empty
        else:
            raise QueueError(ret)
    
    def destroy_ref(self):
        self.sock.write(json.dumps({'cmd': 'quit'}) + '\n')
        self.sock.close()

    def destroy_queue(self):
        self.sock.write(json.dumps({'cmd': 'del',
                                    'name': self.name}) + '\n')
        self.destroy_ref()

    def qsize(self):
        self.sock.write(json.dumps({'cmd': 'size',
                                    'name': self.name}) + '\n')
        ret = json.loads(self.sock.readline())
        if ret['status']:
            return ret['result']
        else:
            raise QueueError(ret)

    def flush(self):
        self.sock.write(json.dumps({'cmd': 'flush',
                                    'name': self.name}) + '\n')
        ret = json.loads(self.sock.readline())
        if ret['status']:
            return True
        else:
            raise QueueError(ret)

    def stats(self):
        self.sock.write(json.dumps({'cmd': 'stats'}) + '\n')

    def report(self):
        self.sock.write(json.dumps({'cmd': 'report'}) + '\n')
