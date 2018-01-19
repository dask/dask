import os

def pytest_ignore_collect(path, config):
    if os.path.split(str(path))[1].startswith("hdfs.py"):
        return True
