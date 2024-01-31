import functools
import operator

from dask_expr._util import _convert_to_list
from dask_expr.io.io import BlockwiseIO, PartitionsFiltered


class ReadCSV(PartitionsFiltered, BlockwiseIO):
    _parameters = [
        "filename",
        "columns",
        "header",
        "dtype_backend",
        "_partitions",
        "storage_options",
        "kwargs",
        "_series",
    ]
    _defaults = {
        "columns": None,
        "header": "infer",
        "dtype_backend": None,
        "kwargs": None,
        "_partitions": None,
        "storage_options": None,
        "_series": False,
    }
    _absorb_projections = True

    @functools.cached_property
    def operation(self):
        from dask.dataframe.io import read_csv

        return read_csv

    @functools.cached_property
    def _ddf(self):
        # Temporary hack to simplify logic

        kwargs = (
            {"dtype_backend": self.dtype_backend}
            if self.dtype_backend is not None
            else {}
        )
        if self.kwargs is not None:
            kwargs.update(self.kwargs)

        columns = _convert_to_list(self.operand("columns"))
        if columns is None:
            pass
        elif "include_path_column" in self.kwargs:
            flag = self.kwargs["include_path_column"]
            if flag is True:
                column_to_remove = "path"
            elif isinstance(flag, str):
                column_to_remove = flag
            else:
                column_to_remove = None

            columns = [c for c in columns if c != column_to_remove]

            if not columns:
                meta = self.operation(
                    self.filename,
                    header=self.header,
                    storage_options=self.storage_options,
                    **kwargs,
                )._meta
                columns = [list(meta.columns)[0]]

        return self.operation(
            self.filename,
            usecols=columns,
            header=self.header,
            storage_options=self.storage_options,
            **kwargs,
        )

    @functools.cached_property
    def _meta(self):
        return self._ddf._meta

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


class ReadTable(ReadCSV):
    @functools.cached_property
    def operation(self):
        from dask.dataframe.io import read_table

        return read_table


class ReadFwf(ReadCSV):
    @functools.cached_property
    def operation(self):
        from dask.dataframe.io import read_fwf

        return read_fwf


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
