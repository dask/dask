import functools
import operator

from dask_expr._util import _convert_to_list
from dask_expr.io.io import BlockwiseIO, PartitionsFiltered


class ReadCSV(PartitionsFiltered, BlockwiseIO):
    _parameters = [
        "filename",
        "columns",
        "header",
        "_partitions",
        "storage_options",
        "_series",
    ]
    _defaults = {
        "columns": None,
        "header": "infer",
        "_partitions": None,
        "storage_options": None,
        "_series": False,
    }
    _absorb_projections = True

    @functools.cached_property
    def _ddf(self):
        # Temporary hack to simplify logic
        import dask.dataframe as dd

        return dd.read_csv(
            self.filename,
            usecols=_convert_to_list(self.operand("columns")),
            header=self.header,
            storage_options=self.storage_options,
        )

    @functools.cached_property
    def _meta(self):
        meta = self._ddf._meta
        if self.operand("columns") is not None:
            return meta[self.columns[0]] if self._series else meta[self.columns]
        return meta

    @functools.cached_property
    def columns(self):
        columns_operand = self.operand("columns")
        if columns_operand is None:
            try:
                return list(self._ddf._meta.columns)
            except AttributeError:
                return []
        else:
            return _convert_to_list(columns_operand)

    def _divisions(self):
        return self._ddf.divisions

    @functools.cached_property
    def _tasks(self):
        return list(self._ddf.dask.to_dict().values())

    def _filtered_task(self, index: int):
        if self._series:
            return (operator.getitem, self._tasks[index], self.columns[0])
        return self._tasks[index]


def to_csv(
    df,
    filename,
    single_file=False,
    encoding="utf-8",
    mode="wt",
    name_function=None,
    compression=None,
    compute=True,
    scheduler=None,
    storage_options=None,
    header_first_partition_only=None,
    compute_kwargs=None,
    **kwargs,
):
    from dask.dataframe.io.csv import to_csv as _to_csv

    return _to_csv(
        df.to_dask_dataframe(),
        filename,
        single_file=single_file,
        encoding=encoding,
        mode=mode,
        name_function=name_function,
        compression=compression,
        compute=compute,
        scheduler=scheduler,
        storage_options=storage_options,
        header_first_partition_only=header_first_partition_only,
        compute_kwargs=compute_kwargs,
        **kwargs,
    )
