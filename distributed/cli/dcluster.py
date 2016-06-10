from warnings import warn

def go():
    print("The `dcluster` command has been deprecated.")
    print("Please use `dask-ssh` in the future.")
    print()
    from distributed.cli.dask_cluster import go
    go()


if __name__ == '__main__':
    go()
