In [1]: from pandas import msgpack

In [2]: import blosc

In [3]: seq = [(i, i % 10, str(i*2)*10) for i in xrange(1000000)]

In [4]: f = open('foo.blk', 'w')

In [5]: from StringIO import StringIO

In [6]: s = StringIO()

In [7]: %time msgpack.pack(seq, s)
CPU times: user 135 ms, sys: 23.8 ms, total: 159 ms
Wall time: 159 ms

In [8]: s.seek(0)

In [9]: text = s.read()

In [10]: len(text)
Out[10]: 74311999

In [11]: %time compressed = blosc.compress(text, 1)
CPU times: user 177 ms, sys: 0 ns, total: 177 ms
Wall time: 49.2 ms

In [12]: len(compressed)
Out[12]: 12293671

In [13]: %time f.write(compressed)
CPU times: user 446 µs, sys: 15.1 ms, total: 15.5 ms
Wall time: 16.1 ms

In [14]: len(text) / (0.159 + 0.049 + 0.016) / 1e6  # MB/s
Out[14]: 331.7499955357142

