from __future__ import print_function, division, absolute_import

import paramiko
from time import sleep

from toolz import assoc


def start_center(addr):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(addr)

    channel = ssh.invoke_shell()
    channel.settimeout(20)
    sleep(0.1)
    channel.send('dcenter --host %s\n' % addr)
    channel.recv(10000)

    return {'client': ssh, 'channel': channel}


def start_worker(center_addr, addr):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(addr)

    channel = ssh.invoke_shell()
    channel.settimeout(20)
    sleep(0.1)
    channel.send('dworker %s:8787\n --host %s' % (center_addr, addr))
    channel.recv(10000)

    return {'client': ssh, 'channel': channel}


class Cluster(object):
    def __init__(self, center, workers):
        self.center = assoc(start_center(center), 'address', center)
        self.workers = [assoc(start_worker(center, worker), 'address', worker)
                        for worker in workers]
        sleep(1)
        self.report()

    def report(self):
        self.center['channel'].settimeout(1)
        print("Center\n------")
        print(self.center['channel'].recv(10000).decode())
        for worker in self.workers:
            channel = worker['channel']
            address = worker['address']
            channel.settimeout(1)
            print("Worker: %s\n---------" % address+ '-' * len(address))
            print(channel.recv(10000).decode())

    def add_worker(self, address):
        self.workers.append(assoc(start_worker(self.center['address'], address),
                            'address', address))

    def close(self):
        for d in self.workers:
            d['channel'].close()
        self.center['channel'].close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
