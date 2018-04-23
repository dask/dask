from dask.utils import tmpfile
from dask.config import merge, collect_yaml, collect_env
import yaml
import os


def test_merge():
    a = {'x': 1, 'y': {'a': 1}}
    b = {'x': 2, 'z': 3, 'y': {'b': 2}}

    expected = {
        'x': 2,
        'y': {'a': 1, 'b': 2},
        'z': 3
    }

    c = merge(a, b)
    assert c == expected


def test_collect():
    a = {'x': 1, 'y': {'a': 1}}
    b = {'x': 2, 'z': 3, 'y': {'b': 2}}

    expected = {
        'x': 2,
        'y': {'a': 1, 'b': 2},
        'z': 3,
    }

    with tmpfile(extension='yaml') as fn1:
        with tmpfile(extension='yaml') as fn2:
            a = {'x': 1, 'y': {'a': 1}}
            b = {'x': 2, 'z': 3, 'y': {'b': 2}}
            with open(fn1, 'w') as f:
                yaml.dump(a, f)
            with open(fn2, 'w') as f:
                yaml.dump(b, f)

            config = merge(*collect_yaml(paths=[fn1, fn2]))
            assert config == expected


def test_env():
    os.environ['DASK_A_B'] = '123'
    os.environ['DASK_C'] = 'True'
    os.environ['DASK_D'] = 'hello'
    try:
        env = collect_env()
        assert env == {
            'a-b': 123,
            'c': True,
            'd': 'hello'
        }
    finally:
        del os.environ['DASK_A_B']
        del os.environ['DASK_C']
        del os.environ['DASK_D']
