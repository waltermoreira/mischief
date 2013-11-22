import sys
import os

import pytest

# Append directory where the current package
sys.path.insert(
    0, os.path.join('../../', os.path.dirname(os.path.abspath(__file__))))

import mischief.actors.pipe as p
import mischief.actors.namebroker as n

@pytest.yield_fixture(scope='session')
def namebroker():
    """Start and return a client for the namebroker."""
    client = n.NameBrokerClient()
    nb = None
    if not client.is_server_alive():
        nb = n.NameBroker()
        nb.start()
    yield client
    if nb is not None:
        nb.stop()
    

