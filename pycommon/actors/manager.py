from gevent import spawn, socket
from gevent.server import StreamServer
from gevent.queue import Queue, Empty
from gevent.coros import RLock
import multiprocessing
import json
import socket as py_socket
import time
import errno
from pycommon import config
import os

(IP, PORT) = config.get_manager_address()

import logging
mgr_logger = logging.getLogger('manager')
handler = logging.handlers.RotatingFileHandler(os.path.join(os.environ['HET2_DEPLOY'], 'log/gui', 'manager.log'))
formatter = logging.Formatter("%(asctime)s %(levelname)-8s [%(filename)30s:%(lineno)-4s] - %(message)s")
handler.setFormatter(formatter)
mgr_logger.addHandler(handler)
mgr_logger.setLevel(logging.DEBUG)

class Manager(object):

    def __init__(self, address=(IP, PORT)):
        self.server = StreamServer(address, self.handle_request)
        self.queues = {}
        self.conns = 0
        self.connections = {}
        self.address = address

    def get_report(self):
        result = {}
        for q in self.queues:
            queue = self.queues[q]
            result[q] = queue.qsize()
        return result

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
        except (py_socket.error, IOError):
            # Errors mean the connection is NOT alive
            return False

    def _serve_forever(self):
        try:
            self.server.serve_forever()
        except KeyboardInterrupt:
            print 'Manager received Control-C. Quitting...'
            
    def start(self):
        if self._is_alive():
            return
        server_proc = multiprocessing.Process(target=self._serve_forever)
        server_proc.daemon = True
        server_proc.start()
        while not self._is_alive():
            time.sleep(0.1)
        return server_proc

    def stop(self):
        try:
            s = py_socket.create_connection(self.address)
            write_to(s.makefile('w', 0), json.dumps({'cmd': 'stop_server'}), sock=s)
        except (py_socket.error, IOError):
            # Ignore socket errors when stopping
            pass

    def _cmd_put(self, stream, obj):
        self.put(obj['name'], obj['arg'])

    def _cmd_get(self, stream, obj):
        timeout = obj.get('timeout', None)
        res = self.get(obj['name'], timeout=timeout)
        write_to(stream, json.dumps(res) + '\n')

    def _cmd_quit(self, stream, obj):
        # finish the greenlet attending this socket
        raise StopIteration

    def _cmd_del(self, stream, obj):
        try:
            # send None to the queue to help it clean any thread that
            # is reading the queue
            self.queues[obj['name']].put(None)
            del self.queues[obj['name']]
        except KeyError:
            # ignore if the queue is already removed
            pass

    def _cmd_touch(self, stream, obj):
        self.touch(obj['name'])

    def _cmd_stats(self, stream, obj):
        self.stats()

    def _cmd_size(self, stream, obj):
        res = self.size(obj['name'])
        write_to(stream, json.dumps(res) + '\n')

    def _cmd_flush(self, stream, obj):
        self.flush(obj['name'])
        write_to(stream, json.dumps({'status': True}) + '\n')

    def _cmd_stop_server(self, stream, obj):
        self.server.stop()
        return

    def _cmd_report(self, stream, obj):
        # print report to screen
        self.report()

    def _cmd_get_report(self, stream, obj):
        # return report to output stream (for debugging)
        report = self.get_report()
        write_to(stream, json.dumps(report) + '\n')

    def _cmd_unknown(self, stream, obj):
        write_to(stream, json.dumps({'status': False,
                                     'type': 'unknown_cmd'}) + '\n')
        
    def handle_request(self, sock, address):
        self.conns += 1
        stream = sock.makefile('w', bufsize=0)
        try:
            while True:
                obj = None
                try:
                    line = readline_from(stream, sock=sock)
                    if not line:
                        return
                    obj = json.loads(line)
                    cmd = obj['cmd']
                    cmd_f = getattr(self, '_cmd_%s' %cmd, self._cmd_unknown)
                    cmd_f(stream, obj)
                except ValueError as exc:
                    # wrong json object
                    write_to(stream, json.dumps({'status': False,
                                                 'type': 'not_a_json',
                                                 'msg': exc.message,
                                                 'line': line}) + '\n',
                             sock=sock)
                    return
                except StopIteration:
                    return
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
        self.sock = py_socket.create_connection(address)
        self.stream = self.sock.makefile('w', 0)
        write_to(self.stream, json.dumps({'cmd': 'touch',
                                    'name': self.name}) + '\n', sock=self.sock)
        
    def put(self, obj):
        write_to(self.stream, json.dumps({'cmd': 'put',
                                    'name': self.name,
                                    'arg': obj}) + '\n', sock=self.sock)

    def get(self, timeout=None):
        write_to(self.stream, json.dumps({'cmd': 'get',
                                    'timeout': timeout,
                                    'name': self.name}) + '\n', sock=self.sock)
        try:
            ret = json.loads(readline_from(self.stream, sock=self.sock))
        except ValueError:
            return False
        if ret['status']:
            return ret['result']
        if ret['type'] == 'empty':
            raise Empty
        else:
            raise QueueError(ret)
    
    def destroy_ref(self):
        write_to(self.stream, json.dumps({'cmd': 'quit'}) + '\n', sock=self.sock)
        try:
            self.stream.close()
            self.sock.close()
        except:
            mgr_logger.debug('Exception while closing:')
            import traceback
            mgr_logger.debug(traceback.format_exc())

    def destroy_queue(self):
        write_to(self.stream, json.dumps({'cmd': 'del',
                                    'name': self.name}) + '\n', sock=self.sock)
        self.destroy_ref()

    def qsize(self):
        write_to(self.stream, json.dumps({'cmd': 'size',
                                    'name': self.name}) + '\n', sock=self.sock)
        try:
            ret = json.loads(readline_from(self.stream, sock=self.sock))
        except ValueError:
            return False
        if ret['status']:
            return ret['result']
        else:
            raise QueueError(ret)

    def flush(self):
        write_to(self.stream, json.dumps({'cmd': 'flush',
                                    'name': self.name}) + '\n', sock=self.sock)
        try:
            ret = json.loads(readline_from(self.stream, sock=self.sock))
        except ValueError:
            return False
        if ret['status']:
            return True
        else:
            raise QueueError(ret)

    def stats(self):
        write_to(self.stream, json.dumps({'cmd': 'stats'}) + '\n', sock=self.sock)

    def report(self):
        write_to(self.stream, json.dumps({'cmd': 'report'}) + '\n', sock=self.sock)

def write_to(stream, data, sock=None, retries=3, sleep_func=time.sleep):
    """
    Send data to a socket, retrying if necessary
    """
    try:
        try:
            address = '%s writes to %s' %(sock.getsockname(), sock.getpeername())
        except:
            address = '...'
        mgr_logger.debug('[%s] %s' %(address, data))
        stream.write(data)
    except socket.error as exc:
        if exc.errno in (errno.EPIPE, errno.ECONNRESET):
            mgr_logger.debug('Got broken pipe when I was about to write: %s' %(data,))
        else:
            mgr_logger.debug('socket.error: %s' %(exc.errno,))
        import traceback
        mgr_logger.debug(traceback.format_exc())
    except:
        mgr_logger.debug('Was about to write: %s' %(data,))
        import traceback
        mgr_logger.debug(traceback.format_exc())

def readline_from(stream, sock=None, retries=3):
    try:
        try:
            address = 'from %s reads into %s' %(sock.getpeername(), sock.getsockname())
        except:
            address = '...'
        x = stream.readline()   
        mgr_logger.debug('[%s] %s' %(address, x))
        return x 
    except:
        mgr_logger.debug('got this while reading')
        import traceback
        mgr_logger.debug(traceback.format_exc())
        return ''

