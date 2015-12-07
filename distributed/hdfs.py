""" This file is experimental and may disappear without warning """
from __future__ import print_function, division, absolute_import

import os

from .executor import default_executor
from .utils import ignoring

with ignoring(ImportError):
    import snakebite.protobuf.ClientNamenodeProtocol_pb2 as client_proto
    from snakebite.client import Client


def get_locations(filename, name_host, name_port, data_root='/data/dfs/dn'):
    client = Client(name_host, name_port, use_trash=False)
    files = list(client.ls([filename]))
    return [pair for file in files for pair in find(file, client, data_root)]


def find(f, client, data_root='/data/dfs/dn'):
    request = client_proto.GetBlockLocationsRequestProto()
    request.src = f['path']
    request.length = long(f['length'])
    request.offset = long(0)

    response = client.service.getBlockLocations(request)

    return [{'block': block,
             'path': get_local_path(block, data_root),
             'hosts': [location.id.ipAddr for location in block.locs]}
             for block in response.locations.blocks]


def get_local_path(block, data_root='/data/dfs/dn'):
    pool = block.b.poolId
    Id = block.b.blockId
    loc = idtoBlockdir(Id)
    return "{}/current/{}/current/finalized/{}/blk_{}".format(
                    data_root, pool, loc, Id)


BLOCK_SUBDIR_PREFIX = 'subdir'
def idtoBlockdir(blockId):
    d1 = str(((blockId >> 16) & 0xff))
    d2 = str(((blockId >> 8) & 0xff))
    pathd1 = BLOCK_SUBDIR_PREFIX+d1
    pathd2 = BLOCK_SUBDIR_PREFIX+d2
    path = os.path.join(pathd1, pathd2)
    return path


def get_data_root():
    confd = os.environ.get('HADOOP_CONF_DIR', os.environ.get('HADOOP_INSTALL',
                           '') + '/hadoop/conf')
    conf = os.sep.join([confd, 'hdfs-site.xml'])
    import xml
    x = xml.etree.ElementTree.fromstring(open(conf).read())
    for e in x:
        if e.find('name').text == 'dfs.datanode.data.dir':
            return e.find('value').text


def map_blocks(func, location, namenode_host, namenode_port,
               data_root='/data/dfs/dn', executor=None, **kwargs):
    """ Map a function over blocks of a location in HDFS

    >>> L = map_blocks(pd.read_csv, '/data/nyctaxi/',
    ...                '192.168.1.100', 9000)  # doctest: +SKIP
    >>> type(L)[0]  # doctest: +SKIP
    Future
    """
    executor = default_executor(executor)
    blocks = get_locations(location, namenode_host, namenode_port, data_root)
    paths = [blk['path'] for blk in blocks]
    hosts = [blk['hosts'] for blk in blocks]
    return executor.map(func, paths, workers=hosts, **kwargs)


def dask_graph(func, location, namenode_host, namenode_port, executor=None,
               **kwargs):
    """ Produce dask graph mapping function over blocks in HDFS

    Inserts HDFS host restrictions into the executor.

    Returns a graph and keys corresponding to function applied to blocks.
    Does not trigger execution.

    >>> dsk, keys = dask_graph(pd.read_csv, '/data/nyctaxi/',
    ...                        '192.168.1.100', 9000)  # doctest: +SKIP
    """
    executor = default_executor(executor)
    blocks = get_locations(location, namenode_host, namenode_port, **kwargs)
    paths = [blk['path'] for blk in blocks]
    hosts = [blk['hosts'] for blk in blocks]
    names = [(funcname(func), path) for path in paths]
    restrictions = dict(zip(names, hosts))

    dsk = {name: (func, path) for name, path in zip(names, paths)}

    executor.send_to_scheduler({'op': 'update-graph',
                                'dsk': {},
                                'keys': [],
                                'restrictions': restrictions})
    return dsk, names


def read_csv(location, namenode_host, namenode_port, executor=None, **kwargs):
    import pandas as pd
    import dask.dataframe as dd
    from dask.compatibility import apply
    executor = default_executor(executor)

    blocks = get_locations(location, namenode_host, namenode_port, **kwargs)
    paths = [blk['path'] for blk in blocks]
    hosts = [blk['hosts'] for blk in blocks]
    name = 'hdfs-read-csv-' + location
    names = [(name, i) for i, _ in enumerate(paths)]

    restrictions = dict(zip(names, hosts))
    executor.send_to_scheduler({'op': 'update-graph',
                                'dsk': {},
                                'keys': [],
                                'restrictions': restrictions})

    future = executor.submit(fill_kwargs, blocks[0]['path'], **kwargs)
    columns, kwargs = future.result()
    rest_kwargs = assoc(kwargs, 'header', None)

    dsk = {(name, i): (apply, pd.read_csv, (path,), rest_kwargs)
           for i, path in enumerate(paths)}
    dsk[(name, 0)] = (apply, pd.read_csv, (paths[0],), kwargs)
    divisions = [None] * (len(paths) + 1)

    return dd.DataFrame(dsk, name, columns, divisions)
