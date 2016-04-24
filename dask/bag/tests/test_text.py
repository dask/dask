from __future__ import print_function, division, absolute_import

import pytest
from toolz import concat, valmap, partial

from dask import compute, get
from dask.utils import filetexts
from dask.bytes import compression
from dask.bag.text import read_text

compute = partial(compute, get=get)


files = {'.test.accounts.1.json':  (b'{"amount": 100, "name": "Alice"}\n'
                                    b'{"amount": 200, "name": "Bob"}\n'
                                    b'{"amount": 300, "name": "Charlie"}\n'
                                    b'{"amount": 400, "name": "Dennis"}\n'),
         '.test.accounts.2.json':  (b'{"amount": 500, "name": "Alice"}\n'
                                    b'{"amount": 600, "name": "Bob"}\n'
                                    b'{"amount": 700, "name": "Charlie"}\n'
                                    b'{"amount": 800, "name": "Dennis"}\n')}

expected = b''.join([files[v] for v in sorted(files)])

from dask.bytes.compression import compress, files as cfiles, seekable_files
fmt_bs = ([(fmt, None) for fmt in cfiles]
        + [(fmt, 10) for fmt in seekable_files]
        + [(fmt, None) for fmt in seekable_files])
fmt_bs_enc = [(fmt, bs, encoding) for fmt, bs in fmt_bs
                                  for encoding in ['ascii', 'utf-8']]


@pytest.mark.parametrize('fmt,bs,encoding', fmt_bs_enc)
def test_read_text(fmt, bs, encoding):
    files2 = valmap(compression.compress[fmt], files)
    with filetexts(files2, mode='b'):
        b = read_text('.test.accounts.*.json', compression=fmt, blocksize=bs,
                encoding=encoding)
        L, = compute(b)
        assert ''.join(L) == expected.decode(encoding)
