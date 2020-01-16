import os
import stat
import sys
from collections import OrderedDict
from contextlib import contextmanager

import pytest

import dask.config
from dask.config import (
    update,
    merge,
    collect,
    collect_yaml,
    collect_env,
    get,
    ensure_file,
    set,
    config,
    rename,
    update_defaults,
    refresh,
    expand_environment_variables,
    canonical_name,
)

from dask.utils import tmpfile

yaml = pytest.importorskip("yaml")


def test_canonical_name():
    c = {"foo-bar": 1, "fizz_buzz": 2}
    assert canonical_name("foo-bar", c) == "foo-bar"
    assert canonical_name("foo_bar", c) == "foo-bar"
    assert canonical_name("fizz-buzz", c) == "fizz_buzz"
    assert canonical_name("fizz_buzz", c) == "fizz_buzz"
    assert canonical_name("new-key", c) == "new-key"
    assert canonical_name("new_key", c) == "new_key"


def test_update():
    a = {"x": 1, "y": {"a": 1}}
    b = {"x": 2, "z": 3, "y": OrderedDict({"b": 2})}
    update(b, a)
    assert b == {"x": 1, "y": {"a": 1, "b": 2}, "z": 3}

    a = {"x": 1, "y": {"a": 1}}
    b = {"x": 2, "z": 3, "y": {"a": 3, "b": 2}}
    update(b, a, priority="old")
    assert b == {"x": 2, "y": {"a": 3, "b": 2}, "z": 3}


def test_merge():
    a = {"x": 1, "y": {"a": 1}}
    b = {"x": 2, "z": 3, "y": {"b": 2}}

    expected = {"x": 2, "y": {"a": 1, "b": 2}, "z": 3}

    c = merge(a, b)
    assert c == expected


def test_collect_yaml_paths():
    a = {"x": 1, "y": {"a": 1}}
    b = {"x": 2, "z": 3, "y": {"b": 2}}

    expected = {"x": 2, "y": {"a": 1, "b": 2}, "z": 3}

    with tmpfile(extension="yaml") as fn1:
        with tmpfile(extension="yaml") as fn2:
            with open(fn1, "w") as f:
                yaml.dump(a, f)
            with open(fn2, "w") as f:
                yaml.dump(b, f)

            config = merge(*collect_yaml(paths=[fn1, fn2]))
            assert config == expected


def test_collect_yaml_dir():
    a = {"x": 1, "y": {"a": 1}}
    b = {"x": 2, "z": 3, "y": {"b": 2}}

    expected = {"x": 2, "y": {"a": 1, "b": 2}, "z": 3}

    with tmpfile() as dirname:
        os.mkdir(dirname)
        with open(os.path.join(dirname, "a.yaml"), mode="w") as f:
            yaml.dump(a, f)
        with open(os.path.join(dirname, "b.yaml"), mode="w") as f:
            yaml.dump(b, f)

        config = merge(*collect_yaml(paths=[dirname]))
        assert config == expected


@contextmanager
def no_read_permissions(path):
    perm_orig = stat.S_IMODE(os.stat(path).st_mode)
    perm_new = perm_orig ^ stat.S_IREAD
    try:
        os.chmod(path, perm_new)
        yield
    finally:
        os.chmod(path, perm_orig)


@pytest.mark.skipif(
    sys.platform == "win32", reason="Can't make writeonly file on windows"
)
@pytest.mark.parametrize("kind", ["directory", "file"])
def test_collect_yaml_permission_errors(tmpdir, kind):
    a = {"x": 1, "y": 2}
    b = {"y": 3, "z": 4}

    dir_path = str(tmpdir)
    a_path = os.path.join(dir_path, "a.yaml")
    b_path = os.path.join(dir_path, "b.yaml")

    with open(a_path, mode="w") as f:
        yaml.dump(a, f)
    with open(b_path, mode="w") as f:
        yaml.dump(b, f)

    if kind == "directory":
        cant_read = dir_path
        expected = {}
    else:
        cant_read = a_path
        expected = b

    with no_read_permissions(cant_read):
        config = merge(*collect_yaml(paths=[dir_path]))
        assert config == expected


def test_env():
    env = {
        "DASK_A_B": "123",
        "DASK_C": "True",
        "DASK_D": "hello",
        "DASK_E__X": "123",
        "DASK_E__Y": "456",
        "DASK_F": '[1, 2, "3"]',
        "DASK_G": "/not/parsable/as/literal",
        "FOO": "not included",
    }

    expected = {
        "a_b": 123,
        "c": True,
        "d": "hello",
        "e": {"x": 123, "y": 456},
        "f": [1, 2, "3"],
        "g": "/not/parsable/as/literal",
    }

    res = collect_env(env)
    assert res == expected


def test_collect():
    a = {"x": 1, "y": {"a": 1}}
    b = {"x": 2, "z": 3, "y": {"b": 2}}
    env = {"DASK_W": 4}

    expected = {"w": 4, "x": 2, "y": {"a": 1, "b": 2}, "z": 3}

    with tmpfile(extension="yaml") as fn1:
        with tmpfile(extension="yaml") as fn2:
            with open(fn1, "w") as f:
                yaml.dump(a, f)
            with open(fn2, "w") as f:
                yaml.dump(b, f)

            config = collect([fn1, fn2], env=env)
            assert config == expected


def test_collect_env_none():
    os.environ["DASK_FOO"] = "bar"
    try:
        config = collect([])
        assert config == {"foo": "bar"}
    finally:
        del os.environ["DASK_FOO"]


def test_get():
    d = {"x": 1, "y": {"a": 2}}

    assert get("x", config=d) == 1
    assert get("y.a", config=d) == 2
    assert get("y.b", 123, config=d) == 123
    with pytest.raises(KeyError):
        get("y.b", config=d)


def test_ensure_file(tmpdir):
    a = {"x": 1, "y": {"a": 1}}
    b = {"x": 123}

    source = os.path.join(str(tmpdir), "source.yaml")
    dest = os.path.join(str(tmpdir), "dest")
    destination = os.path.join(dest, "source.yaml")

    with open(source, "w") as f:
        yaml.dump(a, f)

    ensure_file(source=source, destination=dest, comment=False)

    with open(destination) as f:
        result = yaml.safe_load(f)
    assert result == a

    # don't overwrite old config files
    with open(source, "w") as f:
        yaml.dump(b, f)

    ensure_file(source=source, destination=dest, comment=False)

    with open(destination) as f:
        result = yaml.safe_load(f)
    assert result == a

    os.remove(destination)

    # Write again, now with comments
    ensure_file(source=source, destination=dest, comment=True)

    with open(destination) as f:
        text = f.read()
    assert "123" in text

    with open(destination) as f:
        result = yaml.safe_load(f)
    assert not result


def test_set():
    with set(abc=123):
        assert config["abc"] == 123
        with set(abc=456):
            assert config["abc"] == 456
        assert config["abc"] == 123

    assert "abc" not in config

    with set({"abc": 123}):
        assert config["abc"] == 123
    assert "abc" not in config

    with set({"abc.x": 1, "abc.y": 2, "abc.z.a": 3}):
        assert config["abc"] == {"x": 1, "y": 2, "z": {"a": 3}}
    assert "abc" not in config

    d = {}
    set({"abc.x": 123}, config=d)
    assert d["abc"]["x"] == 123


def test_set_kwargs():
    with set(foo__bar=1, foo__baz=2):
        assert config["foo"] == {"bar": 1, "baz": 2}
    assert "foo" not in config

    # Mix kwargs and dict, kwargs override
    with set({"foo.bar": 1, "foo.baz": 2}, foo__buzz=3, foo__bar=4):
        assert config["foo"] == {"bar": 4, "baz": 2, "buzz": 3}
    assert "foo" not in config

    # Mix kwargs and nested dict, kwargs override
    with set({"foo": {"bar": 1, "baz": 2}}, foo__buzz=3, foo__bar=4):
        assert config["foo"] == {"bar": 4, "baz": 2, "buzz": 3}
    assert "foo" not in config


def test_set_nested():
    with set({"abc": {"x": 123}}):
        assert config["abc"] == {"x": 123}
        with set({"abc.y": 456}):
            assert config["abc"] == {"x": 123, "y": 456}
        assert config["abc"] == {"x": 123}
    assert "abc" not in config


def test_set_hard_to_copyables():
    import threading

    with set(x=threading.Lock()):
        with set(y=1):
            pass


@pytest.mark.parametrize("mkdir", [True, False])
def test_ensure_file_directory(mkdir, tmpdir):
    a = {"x": 1, "y": {"a": 1}}

    source = os.path.join(str(tmpdir), "source.yaml")
    dest = os.path.join(str(tmpdir), "dest")

    with open(source, "w") as f:
        yaml.dump(a, f)

    if mkdir:
        os.mkdir(dest)

    ensure_file(source=source, destination=dest)

    assert os.path.isdir(dest)
    assert os.path.exists(os.path.join(dest, "source.yaml"))


def test_ensure_file_defaults_to_DASK_CONFIG_directory(tmpdir):
    a = {"x": 1, "y": {"a": 1}}
    source = os.path.join(str(tmpdir), "source.yaml")
    with open(source, "w") as f:
        yaml.dump(a, f)

    destination = os.path.join(str(tmpdir), "dask")
    PATH = dask.config.PATH
    try:
        dask.config.PATH = destination
        ensure_file(source=source)
    finally:
        dask.config.PATH = PATH

    assert os.path.isdir(destination)
    [fn] = os.listdir(destination)
    assert os.path.split(fn)[1] == os.path.split(source)[1]


def test_rename():
    aliases = {"foo_bar": "foo.bar"}
    config = {"foo-bar": 123}
    rename(aliases, config=config)
    assert config == {"foo": {"bar": 123}}


def test_refresh():
    defaults = []
    config = {}

    update_defaults({"a": 1}, config=config, defaults=defaults)
    assert config == {"a": 1}

    refresh(paths=[], env={"DASK_B": "2"}, config=config, defaults=defaults)
    assert config == {"a": 1, "b": 2}

    refresh(paths=[], env={"DASK_C": "3"}, config=config, defaults=defaults)
    assert config == {"a": 1, "c": 3}


@pytest.mark.parametrize(
    "inp,out",
    [
        ("1", "1"),
        (1, 1),
        ("$FOO", "foo"),
        ([1, "$FOO"], [1, "foo"]),
        ((1, "$FOO"), (1, "foo")),
        ({1, "$FOO"}, {1, "foo"}),
        ({"a": "$FOO"}, {"a": "foo"}),
        ({"a": "A", "b": [1, "2", "$FOO"]}, {"a": "A", "b": [1, "2", "foo"]}),
    ],
)
def test_expand_environment_variables(inp, out):
    try:
        os.environ["FOO"] = "foo"
        assert expand_environment_variables(inp) == out
    finally:
        del os.environ["FOO"]


def test_env_var_canonical_name(monkeypatch):
    value = 3
    monkeypatch.setenv("DASK_A_B", str(value))
    d = {}
    dask.config.refresh(config=d)
    assert get("a_b", config=d) == value
    assert get("a-b", config=d) == value


def test_get_set_canonical_name():
    c = {"x-y": {"a_b": 123}}

    keys = ["x_y.a_b", "x-y.a-b", "x_y.a-b"]
    for k in keys:
        assert dask.config.get(k, config=c) == 123

    with dask.config.set({"x_y": {"a-b": 456}}, config=c):
        for k in keys:
            assert dask.config.get(k, config=c) == 456

    # No change to new keys in sub dicts
    with dask.config.set({"x_y": {"a-b": {"c_d": 1}, "e-f": 2}}, config=c):
        assert dask.config.get("x_y.a-b", config=c) == {"c_d": 1}
        assert dask.config.get("x_y.e_f", config=c) == 2


@pytest.mark.parametrize("key", ["custom_key", "custom-key"])
def test_get_set_roundtrip(key):
    value = 123
    with dask.config.set({key: value}):
        assert dask.config.get("custom_key") == value
        assert dask.config.get("custom-key") == value


def test_merge_None_to_dict():
    assert dask.config.merge({"a": None, "c": 0}, {"a": {"b": 1}}) == {
        "a": {"b": 1},
        "c": 0,
    }


def test_core_file():
    assert "temporary-directory" in dask.config.config
    assert "dataframe" in dask.config.config
    assert "shuffle-compression" in dask.config.get("dataframe")
