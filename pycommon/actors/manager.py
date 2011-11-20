from gevent import spawn, socket, sleep
from gevent.server import StreamServer
from gevent.queue import Queue, Empty
from gevent.coros import RLock
from textwrap import dedent
from itertools import izip_longest, imap
import struct
import select
import uuid
import traceback
import multiprocessing
import json
import socket as py_socket
import time
import errno
from pycommon import config, log
import os

MANAGER_ADDRESS = 'manager'

logger = log.setup('manager', 'to_console')

class Manager(object):

    def __init__(self, address=None):
        create_pipe(MANAGER_ADDRESS)
        self.server = open_read_pipe(MANAGER_ADDRESS)
        # open dummy file descriptor to avoid getting an EOF
        self.dummy = open_write_pipe(MANAGER_ADDRESS)

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
            while True:
                data = self.server.readline()
                if not data:
                    sleep(0.01)
                    continue
                spawn(self.handle_request, data)
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
        logger.debug('>>> Manager started with pid: %s' %(server_proc.pid,))
        return server_proc

    def stop(self):
        try:
            s = py_socket.create_connection(self.address)
            write_to(s.makefile('w', 0), json.dumps({'cmd': 'stop_server'}))
        except (py_socket.error, IOError):
            # Ignore socket errors when stopping
            pass

    def _cmd_put(self, stream, obj):
        self.put(obj['name'], obj['arg'])

    def _cmd_get(self, stream, obj):
        timeout = obj.get('timeout', None)
        res = self.get(obj['name'], timeout=timeout)
        write_to(stream, json.dumps(res))

    def _cmd_quit(self, stream, obj):
        # finish the greenlet attending this socket
        raise StopIteration

    def _cmd_del(self, stream, obj):
        try:
            # send None to the queue to help it clean any thread that
            # is reading the queue
            logger.debug('Deleting queue %s' %(obj['name'],))
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
        write_to(stream, json.dumps(res))

    def _cmd_flush(self, stream, obj):
        self.flush(obj['name'])
        write_to(stream, json.dumps({'status': True}))

    def _cmd_stop_server(self, stream, obj):
        self.server.stop()
        logger.debug('Manager stopped')
        return

    def _cmd_report(self, stream, obj):
        # print report to screen
        self.report()

    def _cmd_get_report(self, stream, obj):
        # return report to output stream (for debugging)
        report = self.get_report()
        write_to(stream, json.dumps(report))

    def _cmd_unknown(self, stream, obj):
        write_to(stream, json.dumps({'status': False,
                                     'type': 'unknown_cmd'}))
        
    def handle_request(self, data_json):
        self.conns += 1
        try:
            data = json.loads(data_json)
            obj = data['payload']
            stream = open_write_pipe(data['pipe'])

            cmd = obj['cmd']
            cmd_f = getattr(self, '_cmd_%s' %cmd, self._cmd_unknown)
            cmd_f(stream, obj)
        except ValueError as exc:
            # wrong json object
            write_to(stream, json.dumps({'status': False,
                                         'type': 'not_a_json',
                                         'msg': exc.message,
                                         'line': data_json}))
        except StopIteration:
            pass
        finally:
            self.conns -= 1

    def flush(self, name):
        """
        Discard all elements in the queue by creating a new one. The
        previous one should be garbage collected.
        """
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
        logger.debug('num queues: %s' %(len(self.queues),))
        logger.debug('num connections: %s' %(self.conns,))
        logger.debug('queues: %s' %(self.queues.keys(),))
        
class QueueError(Exception):
    pass

class QueueRef(object):

    RETRIES = 20
    SLEEP = 0.1 # seconds
    
    def __init__(self, name, address=None):
        self.name = name
        # TODO create fifo with name = name
        for i in range(self.RETRIES):
            try:
                self.sock = py_socket.create_connection(address)
                logger.debug('client at: %s' %(self.sock.getsockname(),))
                self.stream = self.sock.makefile('w', 0)
                # connection to manager was successful
                # leave the ``for`` loop
                break
            except py_socket.error:
                # Cannot connect to manager, keep trying
                logger.debug('No connection, retrying time: %d' %i)
                logger.debug(' address = %s' %(address,))
                logger.debug(' socker error was:')
                logger.debug(traceback.format_exc())
                time.sleep(self.SLEEP)
        else:
            logger.debug('>>>>>>>> Lost connection to manager')
            logger.debug(traceback.format_exc())
            raise QueueError(dedent(
            """Couldn't connect to manager at %s:%s
                   Make sure a manager is running and check the file:
                   '%s/etc/actors.conf'
                   to configure the host and port.
            """ %(IP, PORT, os.environ['HET2_DEPLOY'])))
        write_to(self.stream, json.dumps({'cmd': 'touch',
                                          'name': self.name}))
        
    def write_to_manager(self, data):
        pass

    def read_from_manager(self):
        pass
        
    def put(self, obj):
        self.write_to_manager({'cmd': 'put',
                               'name': self.name,
                               'arg': obj})

    def get(self, timeout=None):
        self.write_to_manager({'cmd': 'get',
                               'timeout': timeout,
                               'name': self.name})
        try:
            ret = self.read_from_manager()
        except ValueError:
            return False
        if ret['status']:
            return ret['result']
        if ret['type'] == 'empty':
            raise Empty
        else:
            raise QueueError(ret)
    
    def destroy_ref(self):
        """
        Ask manager to remove traces of this reference to the
        queue. We close the client socket too, to be sure we don't
        leak open descriptors.
        """
        self.write_to_manager({'cmd': 'quit'})
        # try:
        #     self.stream.close()
        #     self.sock.close()
        # except:
        #     logger.debug('Exception while destroying reference:')
        #     logger.debug(traceback.format_exc())

    def destroy_queue(self):
        """
        Delete the queue in the manager pointed by this reference, and
        delete the reference.
        """
        self.write_to_manager({'cmd': 'del',
                               'name': self.name})
        self.destroy_ref()

    def qsize(self):
        self.write_to_manager({'cmd': 'size',
                               'name': self.name})
        try:
            ret = self.read_from_manager()
        except ValueError:
            return False
        if ret['status']:
            return ret['result']
        else:
            raise QueueError(ret)

    def flush(self):
        self.write_to_manager({'cmd': 'flush',
                               'name': self.name})
        try:
            ret = self.read_from_manager()
        except ValueError:
            return False
        if ret['status']:
            return True
        else:
            raise QueueError(ret)

    def stats(self):
        self.write_to_manager({'cmd': 'stats'})

    def report(self):
        self.write_to_manager({'cmd': 'report'})

