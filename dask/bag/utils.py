def assert_eq(a, b):
    if hasattr(a, 'compute'):
        a = a.compute(scheduler='sync')
    if hasattr(b, 'compute'):
        b = b.compute(scheduler='sync')

    assert a == b
