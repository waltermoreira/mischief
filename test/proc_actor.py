from pycommon.actors.actor import Actor, ActorRef
import subprocess
import os.path
import sys
import time

def process_actor():
    myself = os.path.abspath(__file__)
    if myself.endswith('.pyc'):
        myself = myself[:-1]
    p = subprocess.Popen(['python', myself])
    time.sleep(2)


class _process_actor(Actor):

    def reply(self, msg):
        sender = ActorRef(msg['reply_to'])
        sender.send({'tag': 'answer',
                     'answer': 5})

    def queue(self, msg):
        sender = ActorRef(msg['reply_to'])
        sender.send({'tag': 'answer',
                     'answer': 2})

    def spawn(self, msg):
        _child('child')
        ActorRef(msg['reply_to']).send({
            'tag': 'reply'
            })
        
    def act(self):
        while True:
            self.receive({
                'reply': 'reply',
                'queue': 'queue',
                'spawn': 'spawn'})

if __name__ == '__main__':
    q = _process_actor('p')
    q.act()
