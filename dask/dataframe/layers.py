import tlz as toolz

from ..core import keys_in_tasks
from ..utils import insert, stringify, stringify_collection_keys
from ..highlevelgraph import Layer


def run_ext_function(func, *args):
    if isinstance(func, bytes):
        # Distributed worker will need to deserialize
        # the function if it is `bytes`
        import pickle

        return pickle.loads(func)(*args)
    return func(*args)


class SimpleShuffleLayer(Layer):
    """Layer-materialization class for shuffle.SimpleShuffleLayer"""

    def __init__(self, annotations=None):
        super().__init__(annotations=annotations)

    @property
    def layer_materialize_module(self):
        return SimpleShuffleLayer.__module__

    @property
    def layer_materialize_class(self):
        return "SimpleShuffleLayer"

    @classmethod
    def __dask_distributed_unpack__(cls, state, dsk, dependencies):
        from distributed.worker import dumps_task

        # msgpack will convert lists into tuples, here
        # we convert them back to lists
        if isinstance(state["column"], tuple):
            state["column"] = list(state["column"])
        if "inputs" in state:
            state["inputs"] = list(state["inputs"])

        # Materialize the layer
        # layer_dsk = dict(cls(**state))
        layer_dsk = cls._construct_graph(**state)

        # Convert all keys to strings and dump tasks
        layer_dsk = {
            stringify(k): stringify_collection_keys(v) for k, v in layer_dsk.items()
        }
        keys = layer_dsk.keys() | dsk.keys()

        # TODO: use shuffle-knowledge to calculate dependencies more efficiently
        deps = {k: keys_in_tasks(keys, [v]) for k, v in layer_dsk.items()}

        return {"dsk": toolz.valmap(dumps_task, layer_dsk), "deps": deps}

    @staticmethod
    def _construct_graph(
        name=None,
        column=None,
        npartitions=None,
        npartitions_input=None,
        ignore_index=None,
        name_input=None,
        meta_input=None,
        parts_out=None,
        concat_func=None,
        getitem_func=None,
        shuffle_group_func=None,
    ):
        """Construct graph for a simple shuffle operation."""

        shuffle_group_name = "group-" + name
        shuffle_split_name = "split-" + name

        dsk = {}
        for part_out in parts_out:
            _concat_list = [
                (shuffle_split_name, part_out, part_in)
                for part_in in range(npartitions_input)
            ]
            dsk[(name, part_out)] = (
                run_ext_function,
                concat_func,
                _concat_list,
                ignore_index,
            )
            for _, _part_out, _part_in in _concat_list:
                dsk[(shuffle_split_name, _part_out, _part_in)] = (
                    run_ext_function,
                    getitem_func,
                    (shuffle_group_name, _part_in),
                    _part_out,
                )
                if (shuffle_group_name, _part_in) not in dsk:
                    dsk[(shuffle_group_name, _part_in)] = (
                        run_ext_function,
                        shuffle_group_func,
                        (name_input, _part_in),
                        column,
                        0,
                        npartitions,
                        npartitions,
                        ignore_index,
                        npartitions,
                    )

        return dsk


class ShuffleLayer(SimpleShuffleLayer):
    """Layer-materialization class for shuffle.ShuffleLayer"""

    @property
    def layer_materialize_class(self):
        return "ShuffleLayer"

    def __dask_distributed_pack__(self, *args, **kwargs):
        ret = super().__dask_distributed_pack__(*args, **kwargs)
        ret["inputs"] = self.inputs
        ret["stage"] = self.stage
        ret["nsplits"] = self.nsplits
        return ret

    @staticmethod
    def _construct_graph(
        name=None,
        column=None,
        npartitions=None,
        npartitions_input=None,
        ignore_index=None,
        name_input=None,
        meta_input=None,
        parts_out=None,
        concat_func=None,
        getitem_func=None,
        shuffle_group_func=None,
        inputs=None,
        stage=None,
        nsplits=None,
    ):
        """Construct graph for a "rearrange-by-column" stage."""

        shuffle_group_name = "group-" + name
        shuffle_split_name = "split-" + name

        dsk = {}
        inp_part_map = {inp: i for i, inp in enumerate(inputs)}
        for part in parts_out:

            out = inputs[part]

            _concat_list = []  # get_item tasks to concat for this output partition
            for i in range(nsplits):
                # Get out each individual dataframe piece from the dicts
                _inp = insert(out, stage, i)
                _idx = out[stage]
                _concat_list.append((shuffle_split_name, _idx, _inp))

            # concatenate those pieces together, with their friends
            dsk[(name, part)] = (
                run_ext_function,
                concat_func,
                _concat_list,
                ignore_index,
            )

            for _, _idx, _inp in _concat_list:
                dsk[(shuffle_split_name, _idx, _inp)] = (
                    run_ext_function,
                    getitem_func,
                    (shuffle_group_name, _inp),
                    _idx,
                )

                if (shuffle_group_name, _inp) not in dsk:

                    # Initial partitions (output of previous stage)
                    _part = inp_part_map[_inp]
                    if stage == 0:
                        if _part < npartitions_input:
                            input_key = (name_input, _part)
                        else:
                            # In order to make sure that to_serialize() serialize the
                            # empty dataframe input, we add it as a key.
                            input_key = (shuffle_group_name, _inp, "empty")
                            dsk[input_key] = meta_input
                    else:
                        input_key = (name_input, _part)

                    # Convert partition into dict of dataframe pieces
                    dsk[(shuffle_group_name, _inp)] = (
                        run_ext_function,
                        shuffle_group_func,
                        input_key,
                        column,
                        stage,
                        nsplits,
                        npartitions_input,
                        ignore_index,
                        npartitions,
                    )

        return dsk
