from os import environ, path

import pytest


pytest.main(['-vv', path.join(environ['SRC_DIR'],'dask')])
