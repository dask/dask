from dask.frame.esort import emerge, shard
import numpy as np

def test_shard():
    result = list(shard(3, np.arange(10)))
    assert result[0].tolist() == [0, 1, 2]
    assert result[1].tolist() == [3, 4, 5]
    assert result[2].tolist() == [6, 7, 8]
    assert result[3].tolist() == [9]


def test_emerge():
    seqs = [np.random.random(size=(np.random.randint(100))) for i in range(5)]
    sorteds = [np.sort(x) for x in seqs]
    chunks = [shard(5, x) for x in sorteds]

    out = np.empty(shape=(sum(len(x) for x in seqs),))

    emerge(chunks, out=out)

    assert (out == np.sort(np.concatenate(seqs))).all()
