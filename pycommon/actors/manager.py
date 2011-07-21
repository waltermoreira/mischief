from gevent import spawn, socket
from gevent.server import StreamServer
from gevent.queue import Queue, Empty
import multiprocessing
import json

class Manager(object):

    def __init__(self, address=('localhost', 5123)):
        self.server = StreamServer(address, self.handle_request)
        self.queues = {}
        self.conns = 0
        
    def start(self):
        server_proc = multiprocessing.Process(target=self.server.serve_forever)
        server_proc.daemon = True
        server_proc.start()
        return server_proc

    def handle_request(self, sock, address):
        print 'handling request'
        self.conns += 1
        stream = sock.makefile('w', bufsize=0)
        while True:
            obj = json.loads(stream.readline())
            cmd = obj['cmd']
            if cmd == 'put':
                print 'got put', obj['name'], obj['arg']
                self.put(obj['name'], obj['arg'])
            elif cmd == 'get':
                print 'got get', obj['name']
                timeout = obj.get('timeout', None)
                res = self.get(obj['name'], timeout=timeout)
                print 'will return', res
                stream.write(json.dumps(res) + '\n')
            elif cmd == 'quit':
                print 'leaving request handler'
                self.conns -= 1
                return
            elif cmd == 'del':
                print 'destroying queue'
                del self.queues[obj['name']]
            elif cmd == 'stats':
                self.stats()
            else:
                stream.write(json.dumps({'status': False,
                                         'type': 'unknown_cmd'}) + '\n')
            
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

    def put(self, name, obj):
        q = self.queues.setdefault(name, Queue())
        q.put(obj)

    def stats(self):
        print 'num queues', len(self.queues)
        print 'num connections', self.conns

class QueueError(Exception):
    pass

class QueueRef(object):

    def __init__(self, name):
        self.name = name
        s = socket.create_connection(('localhost', 5123))
        self.sock = s.makefile('w', bufsize=0)
        
    def put(self, obj):
        self.sock.write(json.dumps({'cmd': 'put',
                                    'name': self.name,
                                    'arg': obj}) + '\n')

    def get(self, timeout=None):
        self.sock.write(json.dumps({'cmd': 'get',
                                    'timeout': timeout,
                                    'name': self.name}) + '\n')
        print 'ref will read from server'
        ret = json.loads(self.sock.readline())
        print 'read', ret
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
        
    def stats(self):
        self.sock.write(json.dumps({'cmd': 'stats'}) + '\n')
