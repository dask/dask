def pytest_ignore_collect(path, config):
    if 'fft.py' in str(path):
        return True
