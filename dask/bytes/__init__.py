from distutils.version import LooseVersion

try:
    import fsspec
except ImportError as e:
    fsspec = None

if fsspec is None or LooseVersion(fsspec.__version__) < LooseVersion("0.3.3"):
    raise ImportError(
        "fsspec is required to use any file-system functionality."
        " Please install using\n"
        "conda install -c conda-forge 'fsspec>=0.3.3'\n"
        "or\n"
        "python -m pip install 'fsspec>=0.3.3'"
    )

from .core import read_bytes, open_file, open_files
