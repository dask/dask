import paramiko

from toolz import assoc


def start_center(addr):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(addr)

    channel = ssh.invoke_shell()
    channel.recv(1000)
    channel.send('dcenter\n')

    return {'client': ssh, 'channel': channel}


def start_worker(addr, center_addr):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(addr)

    channel = ssh.invoke_shell()
    channel.recv(1000)
    channel.send('dworker %s:8787\n' % center_addr)

    return {'client': ssh, 'channel': channel}


class Cluster(object):
    def __init__(self, center, workers):
        self.center = assoc(start_center(center), 'address', center)
        self.workers = [assoc(start_worker(center, worker), 'address', worker)
                        for worker in workers]

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
