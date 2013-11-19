import zmq

from flexmock import flexmock
import pytest
import mischief.actors.pipe as p
import mischief.actors.namebroker as n
from mischief.exceptions import PipeEmpty
from mischief.zmq_tools import zmq_socket

def test_get_local_ip():
    locals = ['localhost', '127.0.0.1', '127.0.0.2']
    externals = ['google.com', '8.8.8.8']
    for local in locals:
        x = p.get_local_ip(local)
        assert x == '127.0.0.1'
    for external in externals:
        x = p.get_local_ip(external)
        assert x == '' or not x.startswith('127.')

def test_is_local_ip():
    locals = ['localhost', '127.0.0.1']
    externals = ['google.com', '8.8.8.8']
    for local in locals:
        assert p.is_local_ip(local)
    for external in externals:
        assert not p.is_local_ip(external)

def test_receiver_local():
    with p.Receiver('foo', use_remote=False) as r:
        assert r.address()[-1] is None
        assert r.qsize() == 0
        with pytest.raises(PipeEmpty):
            r.get(timeout=0)

def test_receiver_remote(namebroker):
    with p.Receiver('foo', use_remote=True) as r:
        port = r.address()[-1]
        assert isinstance(port, int) and port > 0
        assert r.qsize() == 0
        with pytest.raises(PipeEmpty):
            r.get(timeout=0)

def test_receiver_register_unregister(namebroker):
    """Check that pipe register and unregister itself."""
    flexmock(n.NameBrokerClient)
    class Msg(object):
        def __init__(self, tag, name):
            self.tag = tag
            self.name = name
        def __eq__(self, other):
            return other['__tag__'] == self.tag and other['__name__'] == self.name
        def __repr__(self):
            return repr((self.tag, self.name))

    n.NameBrokerClient.should_receive('send').with_args(
        'localhost', Msg('register', 'foo')).once()
    n.NameBrokerClient.should_receive('send').with_args(
        'localhost', Msg('unregister', 'foo')).once()
    with p.Receiver('foo', use_remote=True) as r:
        pass

def test_receiver_ping(namebroker):
    with p.Receiver('foo') as r, p.Receiver('bar') as b:
        with p.Sender(r.address()) as s:
            s.put({'__tag__': '__ping__',
                   'reply_to': b.address()})
        assert b.get()['__tag__'] == '__pong__'

def test_receiver_address(namebroker):
    with p.Receiver('foo') as r, p.Receiver('bar') as b:
        with p.Sender(r.address()) as s:
            s.put({'__tag__': '__address__',
                   'reply_to': b.address()})
        assert tuple(b.get()['address']) == tuple(r.address())

def test_receiver_low_level_ping(namebroker):
    with p.Receiver('foo') as r, \
         zmq_socket(zmq.PULL) as zr, \
         zmq_socket(zmq.PUSH) as zs:
        zr.bind('ipc://{}'.format(p.path_to('bar')))
        zs.connect('ipc://{}'.format(p.path_to('foo')))
        zs.send_json({'__tag__': '__low_level_ping__',
                      'reply_to': 'ipc://{}'.format(p.path_to('bar'))})
        assert zr.recv_json()['__tag__'] == '__pong__'
        
def test_receiver_other_data(namebroker):
    with p.Receiver('foo') as r:
        with p.Sender(r.address()) as s:
            s.put({'__tag__': 'spam'})
        assert r.get() == {'__tag__': 'spam'}
        
        
