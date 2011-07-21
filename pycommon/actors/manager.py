from gevent import spawn, socket
from gevent.server import StreamServer
from gevent.queue import Queue
import multiprocessing
import json

class Manager(object):

    def __init__(self, address=('localhost', 5123)):
        self.server = StreamServer(address, self.handle_request)
        self.queues = {}
        
    def start(self):
        server_proc = multiprocessing.Process(target=self.server.serve_forever)
        server_proc.daemon = True
        server_proc.start()
        return server_proc

    def handle_request(self, sock, address):
        print 'handling request'
        stream = sock.makefile('w', bufsize=0)
        while True:
            obj = json.loads(stream.readline())
            cmd = obj['cmd']
            if cmd == 'put':
                print 'got put', obj['name'], obj['arg']
                self.put(obj['name'], obj['arg'])
            elif cmd == 'get':
                print 'got get', obj['name']
                res = self.get(obj['name'])
                print 'will return', res
                stream.write(json.dumps(res) + '\n')
            elif cmd == 'quit':
                print 'leaving request handler'
                return
            elif cmd == 'del':
                print 'destroying queue'
                del self.queues[obj['name']]
            else:
                stream.write(json.dumps({'status': False}) + '\n')
            
    def get(self, name):
        try:
            q = self.queues[name]
            return q.get()
        except KeyError:
            return {'status': False,
                    'msg': "queue '%s' not found" %name}

    def put(self, name, obj):
        q = self.queues.setdefault(name, Queue())
        q.put(obj)
    
class QueueRef(object):

    def __init__(self, name):
        self.name = name
        s = socket.create_connection(('localhost', 5123))
        self.sock = s.makefile('w', bufsize=0)
        
    def put(self, obj):
        self.sock.write(json.dumps({'cmd': 'put',
                                    'name': self.name,
                                    'arg': obj}) + '\n')
        self.sock.flush()

    def get(self):
        self.sock.write(json.dumps({'cmd': 'get',
                                    'name': self.name}) + '\n')
        self.sock.flush()
        print 'ref will read from server'
        ret = json.loads(self.sock.readline())
        print 'read', ret
        self.sock.flush()
        return ret
    
    def destroy_ref(self):
        self.sock.write(json.dumps({'cmd': 'quit'}) + '\n')
        self.sock.close()

    def destroy_queue(self):
        self.sock.write(json.dumps({'cmd': 'del',
                                    'name': self.name}) + '\n')
        self.destroy_ref()
        
