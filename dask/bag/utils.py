from .. import config


def assert_eq(a, b, scheduler=None):
    if scheduler is None:
        scheduler = config.get("testing.assert-eq.scheduler")

    if hasattr(a, "compute"):
        a = a.compute(scheduler=scheduler)
    if hasattr(b, "compute"):
        b = b.compute(scheduler=scheduler)

    assert a == b
