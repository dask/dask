import re
import sys

import pytest
from toolz import first

from distributed.versions import get_versions, error_message
from distributed import Client, Worker, LocalCluster
from distributed.utils_test import gen_cluster, loop  # noqa: F401


# if one of the nodes reports this version, there's a mismatch
mismatched_version = get_versions()
mismatched_version["packages"]["distributed"] = "0.0.0.dev0"

# for really old versions, the `package` key is missing - version is UNKNOWN
key_err_version = {}

# if no key is available for one package, we assume it's MISSING
missing_version = get_versions()
del missing_version["packages"]["distributed"]

# if a node doesn't report any version info, we treat them as UNKNOWN
# the happens if the node is pre-32cb96e, i.e. <=2.9.1
unknown_version = None


@pytest.fixture
def kwargs_matching():
    return dict(
        scheduler=get_versions(),
        workers={f"worker-{i}": get_versions() for i in range(3)},
        client=get_versions(),
    )


def test_versions_match(kwargs_matching):
    assert error_message(**kwargs_matching)["warning"] == ""


@pytest.fixture(params=["client", "scheduler", "worker-1"])
def node(request):
    """Node affected by version mismatch."""
    return request.param


@pytest.fixture(params=["MISMATCHED", "MISSING", "KEY_ERROR", "NONE"])
def effect(request):
    """Specify type of mismatch."""
    return request.param


@pytest.fixture
def kwargs_not_matching(kwargs_matching, node, effect):
    affected_version = {
        "MISMATCHED": mismatched_version,
        "MISSING": missing_version,
        "KEY_ERROR": key_err_version,
        "NONE": unknown_version,
    }[effect]
    kwargs = kwargs_matching
    if node in kwargs["workers"]:
        kwargs["workers"][node] = affected_version
    else:
        kwargs[node] = affected_version
    return kwargs


@pytest.fixture
def pattern(effect):
    """String to match in the right column."""
    return {
        "MISMATCHED": "0.0.0.dev0",
        "MISSING": "MISSING",
        "KEY_ERROR": "UNKNOWN",
        "NONE": "UNKNOWN",
    }[effect]


def test_version_mismatch(node, effect, kwargs_not_matching, pattern):
    column_matching = {"client": 1, "scheduler": 2, "workers": 3}
    msg = error_message(**kwargs_not_matching)
    i = column_matching.get(node, 3)
    assert "Mismatched versions found" in msg["warning"]
    assert "distributed" in msg["warning"]
    assert (
        pattern
        in re.search(
            r"distributed\s+(?:(?:\|[^|\r\n]*)+\|(?:\r?\n|\r)?)+", msg["warning"]
        )
        .group(0)
        .split("|")[i]
        .strip()
    )


def test_scheduler_mismatched_irrelevant_package(kwargs_matching):
    """An irrelevant package on the scheduler can have any version."""
    kwargs_matching["scheduler"]["packages"]["numpy"] = "0.0.0"
    assert "numpy" in kwargs_matching["client"]["packages"]

    assert error_message(**kwargs_matching)["warning"] == ""


def test_scheduler_additional_irrelevant_package(kwargs_matching):
    """An irrelevant package on the scheduler does not need to be present elsewhere."""
    kwargs_matching["scheduler"]["packages"]["pyspark"] = "0.0.0"

    assert error_message(**kwargs_matching)["warning"] == ""


def test_python_mismatch(kwargs_matching):
    kwargs_matching["client"]["packages"]["python"] = "0.0.0"
    msg = error_message(**kwargs_matching)
    assert "Mismatched versions found" in msg["warning"]
    assert "python" in msg["warning"]
    assert (
        "0.0.0"
        in re.search(r"python\s+(?:(?:\|[^|\r\n]*)+\|(?:\r?\n|\r)?)+", msg["warning"])
        .group(0)
        .split("|")[1]
        .strip()
    )


@gen_cluster()
async def test_version_warning_in_cluster(s, a, b):
    s.workers[a.address].versions["packages"]["dask"] = "0.0.0"

    with pytest.warns(None) as record:
        async with Client(s.address, asynchronous=True) as client:
            pass

    assert record
    assert any("dask" in str(r.message) for r in record)
    assert any("0.0.0" in str(r.message) for r in record)

    async with Worker(s.address) as w:
        assert any("workers" in line.message for line in w.logs)
        assert any("dask" in line.message for line in w.logs)
        assert any("0.0.0" in line.message in line.message for line in w.logs)


def test_python_version():
    required = get_versions()["packages"]
    assert "python" in required
    assert required["python"] == ".".join(map(str, sys.version_info))


def test_python_version_error(loop):

    with LocalCluster(1, processes=False, silence_logs=False, loop=loop,) as cluster:
        first(cluster.scheduler.workers.values()).versions["packages"][
            "python"
        ] = "3.5.1"
        with pytest.raises(ImportError) as info:
            with Client(cluster):
                pass

    assert "Python" in str(info.value)
    assert "major" in str(info.value).lower()


def test_lz4_version_error(loop):

    with LocalCluster(
        1, processes=False, silence_logs=False, dashboard_address=None, loop=loop,
    ) as cluster:
        try:
            import lz4  # noqa: F401

            first(cluster.scheduler.workers.values()).versions["packages"]["lz4"] = None
        except ImportError:
            first(cluster.scheduler.workers.values()).versions["packages"][
                "lz4"
            ] = "1.0.0"

        with pytest.raises(ImportError) as info:
            with Client(cluster):
                pass

    assert "lz4" in str(info.value)
