from gevent import spawn, socket
from gevent.server import StreamServer
from gevent.queue import Queue
import multiprocessing
import json

class Manager(object):

    def __init__(self, address):
        self.server = StreamServer(address, self.handle_request)
        self.queues = {}
        
    def start(self):
        server_proc = multiprocessing.Process(target=self.server.serve_forever)
        server_proc.daemon = True
        server_proc.start()
        return server_proc

    def handle_request(self, sock, address):
        print 'handling request'
        stream = sock.makefile('w')
        obj = json.loads(stream.readline())
        if obj['cmd'] == 'put':
            print 'got put', obj['name'], obj['arg']
            self.put(obj['name'], obj['arg'])
        elif obj['cmd'] == 'get':
            print 'got get', obj['name']
            res = self.get(obj['name'])
            print 'will return', res
            stream.write(json.dumps(res) + '\n')
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

    def put(self, obj):
        s = socket.create_connection(('localhost', 5123))
        f = s.makefile('w')
        f.write(json.dumps({'cmd': 'put',
                            'name': self.name,
                            'arg': obj}) + '\n')
        f.close()

    def get(self):
        s = socket.create_connection(('localhost', 5123))
        f = s.makefile('w')
        f.write(json.dumps({'cmd': 'get',
                            'name': self.name}) + '\n')
        f.flush()
        ret = json.loads(f.readline())
        f.close()
        return ret
    
