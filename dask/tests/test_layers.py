import pytest

distributed = pytest.importorskip("distributed")

import sys

from distributed import Client, SchedulerPlugin
from distributed.utils_test import cluster, loop  # noqa F401


class SchedulerImportCheck(SchedulerPlugin):
    """Plugin to help record which modules are imported on the scheduler"""

    def __init__(self, pattern):
        self.pattern = pattern

    async def start(self, scheduler):
        # Record the modules that have been imported when the scheduler starts
        self.start_modules = set()
        for mod in set(sys.modules):
            if not mod.startswith(self.pattern):
                self.start_modules.add(mod)
            else:
                # Maually remove the target library
                sys.modules.pop(mod)


def get_start_modules(dask_scheduler):
    import_check_plugins = [
        p for p in dask_scheduler.plugins if type(p) is SchedulerImportCheck
    ]
    assert len(import_check_plugins) == 1

    plugin = import_check_plugins[0]
    return plugin.start_modules


def _dataframe_shuffle():
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    # Perform a computation using an HLG-based shuffle
    df = pd.DataFrame({"a": range(10), "b": range(10, 20)})
    return dd.from_pandas(df, npartitions=2)


def _dataframe_broadcast_join():
    pd = pytest.importorskip("pandas")
    dd = pytest.importorskip("dask.dataframe")

    # Perform a computation using an HLG-based broadcast join
    df = pd.DataFrame({"a": range(10), "b": range(10, 20)})
    ddf1 = dd.from_pandas(df, npartitions=4)
    ddf2 = dd.from_pandas(df, npartitions=1)
    return ddf1.merge(ddf2, how="left", broadcast=True)


def _array_creation():
    da = pytest.importorskip("dask.array")

    # Perform a computation using HLG-based array creation
    return da.ones((100,)) + da.zeros((100,))


@pytest.mark.parametrize(
    "op,lib",
    [
        (_dataframe_shuffle, "pandas."),
        (_dataframe_broadcast_join, "pandas."),
        (_array_creation, "numpy."),
    ],
)
@pytest.mark.parametrize("optimize_graph", [True, False])
def test_scheduler_highlevel_graph_unpack_import(op, lib, optimize_graph, loop):
    # Test that array/dataframe-specific modules are not imported
    # on the scheduler when an HLG layers are unpacked/materialized.

    with cluster(scheduler_kwargs={"plugins": [SchedulerImportCheck(lib)]}) as (
        scheduler,
        workers,
    ):
        with Client(scheduler["address"], loop=loop) as c:
            # Perform a computation using a HighLevelGraph Layer
            c.compute(op(), optimize_graph=optimize_graph)

            # Get the new modules which were imported on the scheduler during the computation
            end_modules = c.run_on_scheduler(lambda: set(sys.modules))
            start_modules = c.run_on_scheduler(get_start_modules)
            new_modules = end_modules - start_modules

            # Check that the scheduler didn't start with `lib`
            # (otherwise we arent testing anything)
            assert not any(module.startswith(lib) for module in start_modules)

            # Check whether we imported `lib` on the scheduler
            assert not any(module.startswith(lib) for module in new_modules)
