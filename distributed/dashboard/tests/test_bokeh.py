def test_old_import():
    try:
        from distributed.bokeh import BokehScheduler  # noqa: F401
    except ImportError as e:
        assert "distributed.dashboard" in str(e)
