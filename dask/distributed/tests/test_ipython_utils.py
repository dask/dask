from dask.distributed import dask_client_from_ipclient, Worker

def test_dask_client_from_ipclient():
    from IPython.parallel import Client
    c = Client()
    dc = dask_client_from_ipclient(c)
    assert 2 == dc.get({'a': 1, 'b': (lambda x: x + 1, 'a')}, 'b')
    dc.close()
