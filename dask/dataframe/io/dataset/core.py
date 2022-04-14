# def read_dataset(
#     path,
#     columns=None,
#     filters=None,
#     index=None,
#     calculate_divisions=False,
#     storage_options=None,
#     file_format="parquet",
#     engine="pyarrow",
# ):

#     engine = get_engine(engine)

#     # Store initial function arguments
#     input_kwargs = {
#         "columns": columns,
#         "filters": filters,
#         "categories": categories,
#         "index": index,
#         "storage_options": storage_options,
#         "engine": engine,
#         "gather_statistics": gather_statistics,
#         "ignore_metadata_file": ignore_metadata_file,
#         "metadata_task_size": metadata_task_size,
#         "split_row_groups": split_row_groups,
#         "chunksize=": chunksize,
#         "aggregate_files": aggregate_files,
#         **kwargs,
#     }

#     if isinstance(columns, str):
#         input_kwargs["columns"] = [columns]
#         df = read_parquet(path, **input_kwargs)
#         return df[columns]

#     if columns is not None:
#         columns = list(columns)

#     if isinstance(engine, str):
#         engine = get_engine(engine, bool(kwargs))

#     if hasattr(path, "name"):
#         path = stringify_path(path)

#     # Update input_kwargs and tokenize inputs
#     label = "read-parquet-"
#     input_kwargs.update({"columns": columns, "engine": engine})
#     output_name = label + tokenize(path, **input_kwargs)

#     fs, _, paths = get_fs_token_paths(path, mode="rb", storage_options=storage_options)
#     paths = sorted(paths, key=natural_sort_key)  # numeric rather than glob ordering

#     auto_index_allowed = False
#     if index is None:
#         # User is allowing auto-detected index
#         auto_index_allowed = True
#     if index and isinstance(index, str):
#         index = [index]

#     if chunksize or (
#         split_row_groups and int(split_row_groups) > 1 and aggregate_files
#     ):
#         # Require `gather_statistics=True` if `chunksize` is used,
#         # or if `split_row_groups>1` and we are aggregating files.
#         if gather_statistics is False:
#             raise ValueError("read_parquet options require gather_statistics=True")
#         gather_statistics = True

#     read_metadata_result = engine.read_metadata(
#         fs,
#         paths,
#         categories=categories,
#         index=index,
#         gather_statistics=gather_statistics,
#         filters=filters,
#         split_row_groups=split_row_groups,
#         chunksize=chunksize,
#         aggregate_files=aggregate_files,
#         ignore_metadata_file=ignore_metadata_file,
#         metadata_task_size=metadata_task_size,
#         **kwargs,
#     )

#     # In the future, we may want to give the engine the
#     # option to return a dedicated element for `common_kwargs`.
#     # However, to avoid breaking the API, we just embed this
#     # data in the first element of `parts` for now.
#     # The logic below is inteded to handle backward and forward
#     # compatibility with a user-defined engine.
#     meta, statistics, parts, index = read_metadata_result[:4]
#     common_kwargs = {}
#     aggregation_depth = False
#     if len(parts):
#         # For now, `common_kwargs` and `aggregation_depth`
#         # may be stored in the first element of `parts`
#         common_kwargs = parts[0].pop("common_kwargs", {})
#         aggregation_depth = parts[0].pop("aggregation_depth", aggregation_depth)

#     # Parse dataset statistics from metadata (if available)
#     parts, divisions, index, index_in_columns = process_statistics(
#         parts,
#         statistics,
#         filters,
#         index,
#         chunksize,
#         split_row_groups,
#         fs,
#         aggregation_depth,
#     )

#     # Account for index and columns arguments.
#     # Modify `meta` dataframe accordingly
#     meta, index, columns = set_index_columns(
#         meta, index, columns, index_in_columns, auto_index_allowed
#     )
#     if meta.index.name == NONE_LABEL:
#         meta.index.name = None

#     # Set the index that was previously treated as a column
#     if index_in_columns:
#         meta = meta.set_index(index)
#         if meta.index.name == NONE_LABEL:
#             meta.index.name = None

#     if len(divisions) < 2:
#         # empty dataframe - just use meta
#         graph = {(output_name, 0): meta}
#         divisions = (None, None)
#     else:
#         # Create Blockwise layer
#         layer = DataFrameIOLayer(
#             output_name,
#             columns,
#             parts,
#             ParquetFunctionWrapper(
#                 engine,
#                 fs,
#                 meta,
#                 columns,
#                 index,
#                 {},  # All kwargs should now be in `common_kwargs`
#                 common_kwargs,
#             ),
#             label=label,
#             creation_info={
#                 "func": read_parquet,
#                 "args": (path,),
#                 "kwargs": input_kwargs,
#             },
#         )
#         graph = HighLevelGraph({output_name: layer}, {output_name: set()})

#     return new_dd_object(graph, output_name, meta, divisions)


# def check_multi_support(engine):
#     # Helper function to check that the engine
#     # supports a multi-partition read
#     return hasattr(engine, "multi_support") and engine.multi_support()


# def read_parquet_part(fs, engine, meta, part, columns, index, kwargs):
#     """Read a part of a parquet dataset

#     This function is used by `read_parquet`."""
#     if isinstance(part, list):
#         if len(part) == 1 or part[0][1] or not check_multi_support(engine):
#             # Part kwargs expected
#             func = engine.read_partition
#             dfs = [
#                 func(fs, rg, columns.copy(), index, **toolz.merge(kwargs, kw))
#                 for (rg, kw) in part
#             ]
#             df = concat(dfs, axis=0) if len(dfs) > 1 else dfs[0]
#         else:
#             # No part specific kwargs, let engine read
#             # list of parts at once
#             df = engine.read_partition(
#                 fs, [p[0] for p in part], columns.copy(), index, **kwargs
#             )
#     else:
#         # NOTE: `kwargs` are the same for all parts, while `part_kwargs` may
#         #       be different for each part.
#         rg, part_kwargs = part
#         df = engine.read_partition(
#             fs, rg, columns, index, **toolz.merge(kwargs, part_kwargs)
#         )

#     if meta.columns.name:
#         df.columns.name = meta.columns.name
#     columns = columns or []
#     index = index or []
#     df = df[[c for c in columns if c not in index]]
#     if index == [NONE_LABEL]:
#         df.index.name = None
#     return df
