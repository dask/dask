from dask.utils import M


def to_records(df):
    return df.map_partitions(M.to_records)
