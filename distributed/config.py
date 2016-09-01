from __future__ import print_function, division, absolute_import

import os
try:
    import yaml
except ImportError:
    config = {}
else:
    dirname = os.path.dirname(__file__)
    default_path = os.path.join(dirname, 'config.yaml')
    dask_config_path = os.path.join(os.path.expanduser('~'), '.dask', 'config.yaml')


    def ensure_config_file(destination=dask_config_path):
        if not os.path.exists(destination):
            import shutil
            if not os.path.exists(os.path.dirname(destination)):
                os.mkdir(os.path.dirname(destination))
            shutil.copy(default_path, destination)


    def load_config_file(path=dask_config_path):
        if not os.path.exists(path):
            path = default_path
        with open(path) as f:
            text = f.read()
            return yaml.load(text)


    ensure_config_file()
    config = load_config_file()
