from distutils.version import LooseVersion
import fsspec

FSSPEC_VERSION = LooseVersion(fsspec.__version__)
FSSPEC_042 = FSSPEC_VERSION > "0.4.1"
