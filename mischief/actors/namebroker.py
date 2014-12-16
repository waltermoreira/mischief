import traceback
import threading

import zmq
from ..zmq_tools import zmq_socket
from ..exceptions import PipeException
from ..log import setup


logger = setup(to=['file'])


class Server(object):
    """A generic REQ/REP server."""

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.setup()

    def setup(self):
        pass

    @property
    def name(self):
        return '{}-{}:{}'.format(type(self).__name__,
                                 self.ip,
                                 self.port)

    def start(self):
        self.thread = threading.Thread(target=self._server,
                                       args=(logger,))
        self.thread.name = self.name
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        with zmq_socket(zmq.REQ) as s:
            ip = self.ip if self.ip != '*' else 'localhost'
            s.connect('tcp://{}:{}'.format(ip, self.port))
            s.send_json({'__quit__': True})
            s.recv_json()
            self.thread.join()

    def _server(self, logger):
        with zmq_socket(zmq.REP) as s:
            s.bind('tcp://{}:{}'.format(self.ip, self.port))
            while True:
                data = s.recv_json()
                resp = None
                try:
                    if data.get('__quit__'):
                        logger.debug('asked to shutdown')
                        return
                    resp = self.handle(data)
                except Exception:
                    exc = traceback.format_exc()
                    logger.debug('got an exception:')
                    logger.debug(exc)
                    resp = {'exception': exc}
                finally:
                    s.send_json(resp)


class NameBroker(Server):
    """A namebroker server.

    Register, unregister, and provide ports associated to names.

    Create it and start it with::

        x = NameBroker()
        x.start()
        x.stop()

    """

    PORT = 5555

    def __init__(self):
        super(NameBroker, self).__init__('*', self.PORT)

    def setup(self):
        self.names = {}

    def handle(self, data):
        cmd = data['__tag__']
        try:
            return getattr(self, cmd)(data)
        except AttributeError:
            return None

    def get(self, data):
        name = data['__name__']
        port = self.names.get(name, None)
        return {'__port__': port}

    def register(self, data):
        name = data['__name__']
        port = data['__port__']
        self.names[name] = port

    def unregister(self, data):
        name = data['__name__']
        try:
            del self.names[name]
        except KeyError:
            pass

    def list(self, data):
        return self.names

    def ping(self, data):
        return {'__pong__': True}

    def is_alive(self):
        return self.thread.is_alive()


class NameBrokerClient(object):
    """
    Client for the NameBroker server.

    Use as::

        y = NameBrokerClient()
        y.list()

    """

    def __init__(self, at='localhost'):
        self.addr = at

    def is_server_alive(self):
        try:
            resp = self.send(self.addr, {'__tag__': 'ping'})
            return resp['__pong__']
        except PipeException:
            return False

    def register(self, name, port):
        self.send(self.addr,
                  {'__tag__': 'register',
                   '__name__': name,
                   '__port__': port})

    def unregister(self, name):
        self.send(self.addr,
                  {'__tag__': 'unregister',
                   '__name__': name})

    def list(self):
        names = self.send(self.addr,
                          {'__tag__': 'list'})
        if names:
            col = max(map(len, names))
            for name in names:
                logger.debug('{{:>{}}}: {{}}'
                             .format(col)
                             .format(name, names[name]))
        else:
            logger.debug('No registered names')

    @staticmethod
    def send(at, msg, timeout=1000):
        """Send message to NameBroker server at address ``at``."""
        with zmq_socket(zmq.REQ) as s:
            try:
                s.set(zmq.RCVTIMEO, timeout)
                s.connect('tcp://{}:{}'.format(at, NameBroker.PORT))
                s.send_json(msg)
                return s.recv_json()
            except zmq.Again:
                raise PipeException(
                    'cannot connect to NameBroker at {}:{}'
                    .format(at, NameBroker.PORT))
