from warnings import warn

def go():
    print("The `dworker` command has been deprecated.")
    print("Please use `dask-worker` in the future.")
    print()
    from distributed.cli.dask_worker import go
    go()


if __name__ == '__main__':
    go()
