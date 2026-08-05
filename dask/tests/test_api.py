from __future__ import annotations

from types import ModuleType

DASK_EXPORTED_SUBMODULES = {"config", "datasets"}


def test_api():
    """Tests that `dask.__all__` is correct"""
    import dask

    member_dict = vars(dask)
    members = set(member_dict)
    # unexported submodules
    members -= {"tests"}
    members -= {
        m
        for m, mod in member_dict.items()
        if m not in DASK_EXPORTED_SUBMODULES
        if isinstance(mod, ModuleType)
        and mod.__spec__.parent
        and mod.__spec__.parent.startswith("dask")
    }
    # imported utility modules
    members -= {"annotations"}
    # private utilities and `__dunder__` members
    members -= {m for m in members if m.startswith("_")}

    assert set(dask.__all__) == members
