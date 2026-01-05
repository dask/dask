import sys
import pickle

def main():
    try:
        import dask.array as da
    except Exception as e:
        print("IMPORT_ERROR:", repr(e))
        sys.exit(2)

    a = da.ones((10, 10))
    try:
        pickle.dumps(a)
        print("REPRO_OK: dask.array.ones is pickleable")
        sys.exit(0)
    except Exception as e:
        print("REPRO_FAIL:", repr(e))
        sys.exit(1)


if __name__ == "__main__":
    main()

