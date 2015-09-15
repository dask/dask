
import socket

def get_ip():
    return [(s.connect(('8.8.8.8', 80)), s.getsockname()[0], s.close())
        for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]


from contextlib import contextmanager

@contextmanager
def ignoring(*exceptions):
    try:
        yield
    except exceptions:
        pass
