import yaml
import os

import pytest

from dask.config import (update, merge, collect_yaml, collect_env, get,
                         ensure_file, set, config, rename)
from dask.utils import tmpfile


def test_update():
    a = {'x': 1, 'y': {'a': 1}}
    b = {'x': 2, 'z': 3, 'y': {'b': 2}}
    update(b, a)
    assert b == {'x': 1, 'y': {'a': 1, 'b': 2}, 'z': 3}

    a = {'x': 1, 'y': {'a': 1}}
    b = {'x': 2, 'z': 3, 'y': {'a': 3, 'b': 2}}
    update(b, a, priority='old')
    assert b == {'x': 2, 'y': {'a': 3, 'b': 2}, 'z': 3}


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
    env = {'DASK_A_B': '123',
           'DASK_C': 'True',
           'DASK_D': 'hello',
           'DASK_E__X': '123',
           'DASK_E__Y': '456',
           'DASK_F': '[1, 2, "3"]',
           'DASK_G': '/not/parsable/as/literal',
           'FOO': 'not included',
           }

    expected = {
        'a-b': 123,
        'c': True,
        'd': 'hello',
        'e': {'x': 123, 'y': 456},
        'f': [1, 2, "3"],
        'g': '/not/parsable/as/literal',
    }

    assert collect_env(env) == expected


def test_get():
    d = {'x': 1, 'y': {'a': 2}}

    assert get('x', config=d) == 1
    assert get('y.a', config=d) == 2
    assert get('y.b', 123, config=d) == 123
    with pytest.raises(KeyError):
        get('y.b', config=d)


def test_ensure_file():
    a = {'x': 1, 'y': {'a': 1}}
    b = {'x': 123}

    with tmpfile(extension='yaml') as source:
        with tmpfile(extension='yaml') as destination:
            with open(source, 'w') as f:
                yaml.dump(a, f)

            ensure_file(source=source, destination=destination, comment=False)

            with open(destination) as f:
                result = yaml.load(f)

            with open(source) as src:
                with open(destination) as dst:
                    assert src.read() == dst.read()

            assert result == a

            # don't overwrite old config files
            with open(source, 'w') as f:
                yaml.dump(b, f)
            ensure_file(source=source, destination=destination, comment=False)
            with open(destination) as f:
                result = yaml.load(f)

            assert result == a

            os.remove(destination)

            # Write again, now with comments
            ensure_file(source=source, destination=destination, comment=True)
            with open(destination) as f:
                text = f.read()
            assert '123' in text

            with open(destination) as f:
                result = yaml.load(f)

            assert not result


def test_set():
    with set(abc=123):
        assert config['abc'] == 123
        with set(abc=456):
            assert config['abc'] == 456
        assert config['abc'] == 123

    assert 'abc' not in config

    with set({'abc': 123}):
        assert config['abc'] == 123

    with set({'abc.x': 1, 'abc.y': 2, 'abc.z.a': 3}):
        assert config['abc'] == {'x': 1, 'y': 2, 'z': {'a': 3}}

    d = {}
    set({'abc.x': 123}, config=d)
    assert d['abc']['x'] == 123


@pytest.mark.parametrize('mkdir', [True, False])
def test_ensure_file_directory(mkdir):
    a = {'x': 1, 'y': {'a': 1}}
    with tmpfile(extension='yaml') as source:
        with tmpfile() as destination:
            if mkdir:
                os.mkdir(destination)
            with open(source, 'w') as f:
                yaml.dump(a, f)

            ensure_file(source=source, destination=destination)
            assert os.path.isdir(destination)
            [fn] = os.listdir(destination)
            assert os.path.split(fn)[1] == os.path.split(source)[1]


def test_rename():
    aliases = {'foo-bar': 'foo.bar'}
    config = {'foo-bar': 123}
    rename(aliases, config=config)
    assert config == {'foo': {'bar': 123}}
