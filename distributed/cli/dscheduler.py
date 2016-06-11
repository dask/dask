from warnings import warn

def go():
    print("The `dscheduler` command has been deprecated.")
    print("Please use `dask-scheduler` in the future.")
    print()
    from distributed.cli.dask_scheduler import go
    go()


if __name__ == '__main__':
    go()
