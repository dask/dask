from __future__ import absolute_import, print_function, division

import random


def timeseries(
    start='2000-01-01',
    end='2000-01-31',
    freq='1s',
    partition_freq='1d',
    dtypes={'name': str, 'id': int, 'x': float, 'y': float},
    seed=None,
):
    """ Create timeseries dataframe with random data

    Parameters
    ----------
    start : datetime (or datetime-like string)
        Start of time series
    end : datetime (or datetime-like string)
        End of time series
    dtypes : dict
        Mapping of column names to types.
        Valid types include {float, int, str, 'category'}
    freq : string
        String like '2s' or '1H' or '12W' for the time series frequency
    partition_freq : string
        String like '1M' or '2Y' to divide the dataframe into partitions
    seed : int (optional)
        Randomstate seed

    Examples
    --------
    >>> import dask
    >>> df = dask.datasets.timeseries()
    >>> df.head()  # doctest: +SKIP
              timestamp    id     name         x         y
    2000-01-01 00:00:00   967    Jerry -0.031348 -0.040633
    2000-01-01 00:00:01  1066  Michael -0.262136  0.307107
    2000-01-01 00:00:02   988    Wendy -0.526331  0.128641
    2000-01-01 00:00:03  1016   Yvonne  0.620456  0.767270
    2000-01-01 00:00:04   998   Ursula  0.684902 -0.463278
    """
    from dask.dataframe.io.demo import make_timeseries
    return make_timeseries(start=start, end=end, freq=freq,
                           partition_freq=partition_freq,
                           seed=seed, dtypes=dtypes)


def generate_mimesis(field, schema_description, records_per_partition, seed):
    from mimesis.schema import Schema, Field
    field = Field(**field, seed=seed)
    schema = Schema(schema=lambda: schema_description(field))
    for i in range(records_per_partition):
        yield schema.create(iterations=1)


def make_mimesis(field, schema, npartitions, records_per_partition, seed=None):
    import dask.bag as db
    from dask.base import tokenize

    field = field or {}

    if seed is None:
        seed = random.random()

    seeds = db.core.random_state_data_python(npartitions, seed)

    name = 'mimesis-' + tokenize(field, schema, npartitions, records_per_partition, seed)
    dsk = {(name, i): (generate_mimesis, field, schema, records_per_partition, seed)
           for i, seed in enumerate(seeds)}

    return db.Bag(dsk, name, npartitions)


def make_people(npartitions=10, records_per_partition=1000, seed=None, locale='en'):
    """ Make a dataset of random people

    This makes a Dask Bag with dictionary records of randomly generated people.
    This requires the optional library ``mimesis`` to generate records.

    Paramters
    ---------
    npartitions : int
        Number of partitions
    records_per_partition : int
        Number of records in each partition
    seed : int, (optional)
        Random seed
    locale : str
        Language locale, like 'en', 'fr', 'zh', or 'ru'

    Returns
    -------
    b: Dask Bag
    """
    schema = lambda field: {
        'age': field('person.age'),
        'name': (field('person.name'), field('person.surname')),
        'occupation': field('person.occupation'),
        'telephone': field('person.telephone'),
        'address': {'address': field('address.address'),
                    'city': field('address.city')},
        'credt-card': {'number': field('payment.credit_card_number'),
                       'expiration-date': field('payment.credit_card_expiration_date')},
    }

    return make_mimesis({'locale': locale}, schema, npartitions, records_per_partition, seed)
