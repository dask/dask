from dask.dataframe.expr import TimeSeries


def test_basic():
    df = TimeSeries()
    z = df["x"] + df["y"]
    z.visualize("foo.pdf")
    z.compute()
