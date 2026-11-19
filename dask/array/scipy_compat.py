from __future__ import annotations

from packaging.version import Version

try:
    import scipy
except ImportError:
    SCIPY_GE_111 = False
    SCIPY_GE_200 = False
else:
    SCIPY_GE_111 = Version(scipy.__version__) >= Version("1.11")
    SCIPY_GE_200 = Version(scipy.__version__) >= Version("2.0.0.dev0")
