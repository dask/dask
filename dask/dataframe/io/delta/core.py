import json
import re

from fsspec.core import get_fs_token_paths
from pyarrow import dataset as pa_ds

from ....delayed import delayed
from ..io import from_delayed
from ..parquet.core import get_engine, read_parquet
from .utils import schema_from_string

__all__ = "read_delta_table"


class DeltaTable:
    """
    Core DeltaTable Algorithm

    Parameters
    ----------
    path: str
        path of Delta table directory
    version: int, default 0
        DeltaTable Version, used for Time Travelling across the
        different versions of the parquet datasets
    columns: None or list(str)
        Columns to load. If None, loads all.
    engine : str, default 'auto'
        Parquet reader library to use. Options include: 'auto', 'fastparquet',
        'pyarrow', 'pyarrow-dataset', and 'pyarrow-legacy'. Defaults to 'auto',
        which selects the FastParquetEngine if fastparquet is installed (and
        ArrowDatasetEngine otherwise).  If 'pyarrow' or 'pyarrow-dataset' is
        specified, the ArrowDatasetEngine (which leverages the pyarrow.dataset
        API) will be used. If 'pyarrow-legacy' is specified, ArrowLegacyEngine
        will be used (which leverages the pyarrow.parquet.ParquetDataset API).
        NOTE: The 'pyarrow-legacy' option (ArrowLegacyEngine) is deprecated
        for pyarrow>=5.
    checkpoint: int, default None
        DeltaTable protocol aggregates the json delta logs and creates parquet
        checkpoint for every 10 versions.This will save some IO.
        for example:
            if you want to read 106th version of dataset, No need to read all
            the all the 106 json files (which isIO heavy), instead read 100th
            parquet checkpoint which contains all the logs of 100 versions in single
            parquet file and now read the extra 6 json files to match the given
            106th version.
            (so IO reduced from 106 to 7)
    storage_options : dict, default None
        Key/value pairs to be passed on to the file-system backend, if any.
    """

    def __init__(
        self,
        path: str,
        version: int = 0,
        columns=None,
        engine="auto",
        checkpoint=None,
        storage_options=None,
    ):
        self.path = str(path).rstrip("/")
        self.version = version
        self.pq_files = set()
        self.delta_log_path = f"{self.path}/_delta_log"
        self.fs, _, _ = get_fs_token_paths(path, storage_options=storage_options)
        self.engine = engine
        self.columns = columns
        self.checkpoint = (
            checkpoint if checkpoint is not None else self.get_checkpoint_id()
        )
        self.storage_options = storage_options
        self.schema = None

    def get_checkpoint_id(self):
        """
        if _last_checkpoint file exists, returns checkpoint_id else zero
        """
        try:
            with self.fs.open(
                f"{self.delta_log_path}/_last_checkpoint"
            ) as last_checkpoint:
                last_checkpoint_version = json.load(last_checkpoint)["version"]
        except FileNotFoundError:
            last_checkpoint_version = 0
        return last_checkpoint_version

    def get_pq_files_from_checkpoint_parquet(self):
        """
        use checkpoint_id to get logs from parquet files
        """
        if self.checkpoint == 0:
            return
        checkpoint_path = (
            f"{self.delta_log_path}/{self.checkpoint:020}.checkpoint.parquet"
        )
        if not self.fs.exists(checkpoint_path):
            raise ValueError(
                f"Parquet file with the given checkpoint {self.checkpoint} does not exists: "
                f"File {checkpoint_path} not found"
            )

        parquet_checkpoint = read_parquet(
            checkpoint_path, engine=self.engine, storage_options=self.storage_options
        ).compute()

        # reason for this `if condition` was that FastParquetEngine seems to normalizes the json present in each column
        # not sure whether there is a better solution for handling this .
        # `https://fastparquet.readthedocs.io/en/latest/details.html#reading-nested-schema`
        if self.engine.__name__ == "ArrowDatasetEngine":
            for i, row in parquet_checkpoint.iterrows():
                if row["metaData"] is not None:
                    # get latest schema/columns , since delta Lake supports Schema Evolution.
                    self.schema = schema_from_string(row["metaData"]["schemaString"])
                elif row["add"] is not None:
                    self.pq_files.add(f"{self.path}/{row['add']['path']}")

        elif self.engine.__name__ == "FastParquetEngine":
            for i, row in parquet_checkpoint.iterrows():
                if row["metaData.schemaString"]:
                    self.schema = schema_from_string(row["metaData.schemaString"])
                elif row["add.path"]:
                    self.pq_files.add(f"{self.path}/{row['add.path']}")

    def get_pq_files_from_delta_json_logs(self):
        """
        start from checkpoint id, collect logs from every json file until the
        given version
        example:
            checkpoint 10, version 16
                1. read the logs from 10th checkpoint parquet ( using above func)
                2. read logs from json files until version 16
        log Collection:
            for reading the particular version of delta table, We are concerned
            about `add` and `remove` Operation (transaction) only.(which involves
            adding and removing respective parquet file transaction)
        """
        log_files = self.fs.glob(
            f"{self.delta_log_path}/{self.checkpoint // 10:019}*.json"
        )
        if len(log_files) == 0:
            raise RuntimeError(
                f"No Json files found at _delta_log_path:- {self.delta_log_path}"
            )
        log_files = sorted(log_files)
        log_versions = [
            int(re.findall(r"(\d{20})", log_file_name)[0])
            for log_file_name in log_files
        ]
        if (self.version is not None) and (self.version not in log_versions):
            raise ValueError(
                f"Cannot time travel Delta table to version {self.version}, Available versions for given "
                f"checkpoint {self.checkpoint} are {log_versions}"
            )
        for log_file_name, log_version in zip(log_files, log_versions):
            with self.fs.open(log_file_name) as log:
                for line in log:
                    meta_data = json.loads(line)
                    if "add" in meta_data.keys():
                        file = f"{self.path}/{meta_data['add']['path']}"
                        self.pq_files.add(file)

                    elif "remove" in meta_data.keys():
                        remove_file = f"{self.path}/{meta_data['remove']['path']}"
                        if remove_file in self.pq_files:
                            self.pq_files.remove(remove_file)
                    elif "metaData" in meta_data.keys():
                        schema_string = meta_data["metaData"]["schemaString"]
                        self.schema = schema_from_string(schema_string)

                if self.version == int(log_version):
                    break

    def read_delta_dataset(self, f, **kwargs):
        """
        Function to read single parquet file using pyarrow dataset (to support
        schema evolution)
        """
        schema = kwargs.pop("schema", None) or self.schema
        filter = kwargs.pop("filter", None)
        return (
            pa_ds.dataset(
                source=f,
                schema=schema,
                filesystem=self.fs,
                format="parquet",
                partitioning="hive",
            )
            .scanner(filter=filter, columns=self.columns)
            .to_table()
            .to_pandas()
        )

    def read_delta_table(self, **kwargs):
        """
        Reads the list of parquet files in parallel
        """
        if len(self.pq_files) == 0:
            raise RuntimeError("No Parquet files are available")
        parts = [
            delayed(self.read_delta_dataset)(f, **kwargs) for f in list(self.pq_files)
        ]
        return from_delayed(parts)


def read_delta_table(
    path,
    version=None,
    columns=None,
    engine="auto",
    checkpoint=None,
    storage_options=None,
    **kwargs,
):
    """
    Read a Delta Table into a Dask DataFrame

    This reads a list of Parquet files in delta table directory into a
    Dask.dataframe.

    Parameters
    ----------
    path: str
        path of Delta table directory
    version: int, default 0
        DeltaTable Version, used for Time Travelling across the
        different versions of the parquet datasets
    columns: None or list(str)
        Columns to load. If None, loads all.
    engine : str, default 'auto'
        Parquet reader library to use. Options include: 'auto', 'fastparquet',
        'pyarrow', 'pyarrow-dataset', and 'pyarrow-legacy'. Defaults to 'auto',
        which selects the FastParquetEngine if fastparquet is installed (and
        ArrowDatasetEngine otherwise).  If 'pyarrow' or 'pyarrow-dataset' is
        specified, the ArrowDatasetEngine (which leverages the pyarrow.dataset
        API) will be used. If 'pyarrow-legacy' is specified, ArrowLegacyEngine
        will be used (which leverages the pyarrow.parquet.ParquetDataset API).
        NOTE: The 'pyarrow-legacy' option (ArrowLegacyEngine) is deprecated
        for pyarrow>=5.
    checkpoint: int, default None
        DeltaTable protocol aggregates the json delta logs and creates parquet
        checkpoint for every 10 versions.This will save some IO.
        for example:
            if you want to read 106th version of dataset, No need to read all
            the all the 106 json files (which isIO heavy), instead read 100th
            parquet checkpoint which contains all the logs of 100 versions in single
            parquet file and now read the extra 6 json files to match the given
            106th version.
            (so IO reduced from 106 to 7)
    storage_options : dict, default None
        Key/value pairs to be passed on to the file-system backend, if any.
    kwargs: dict,optional
        Some most used parameters can be passed here are:
        1. schema
        2. filter

        schema : pyarrow.Schema
            Used to maintain schema evolution in deltatable.
            delta protocol stores the schema string in the json log files which is converted
             into pyarrow.Schema and used for schema evolution (refer delta/utils.py).
            i.e Based on particular version, some columns can be shown or not shown.
        filter: pyarrow.dataset.Expression
            Can act as both partition as well as row based filter, need to construct the
            pyarrow.dataset.Expression using pyarrow.dataset.Field
            example:
                pyarrow.dataset.field("x")>400
            for more information:
                refer [pyarrow docs](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Expression.html)

    Returns
    -------
    Dask.DataFrame

    Examples
    --------
    >>> df = dd.read_delta_table('s3://bucket/my-delta-table')  # doctest: +SKIP

    """
    engine = get_engine(engine)
    dt = DeltaTable(
        path=path,
        version=version,
        checkpoint=checkpoint,
        columns=columns,
        engine=engine,
        storage_options=storage_options,
    )
    # last_checkpoint_version = dt.get_checkpoint_id()
    dt.get_pq_files_from_checkpoint_parquet()
    dt.get_pq_files_from_delta_json_logs()
    return dt.read_delta_table(columns=columns, **kwargs)
