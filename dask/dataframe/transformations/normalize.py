import dask.dataframe as dd

def normalize_range(df, columns=None, min_val=0, max_val=1):
    """
    Normalize selected numeric columns in a Dask DataFrame to a given range.
    """
    if columns is None:
        columns = df.select_dtypes(include=["number"]).columns

    for col in columns:
        col_min = df[col].min()
        col_max = df[col].max()
        df[col] = (df[col] - col_min) / (col_max - col_min) * (max_val - min_val) + min_val

    return df
