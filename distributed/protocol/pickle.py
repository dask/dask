from __future__ import print_function, division, absolute_import

import logging
import sys

import cloudpickle

if sys.version_info.major == 2:
    import cPickle as pickle
else:
    import pickle

logger = logging.getLogger(__name__)


def _always_use_pickle_for(x):
    mod, _, _ = x.__class__.__module__.partition(".")
    if mod == "numpy":
        import numpy as np

        return isinstance(x, np.ndarray)
    elif mod == "pandas":
        import pandas as pd

        return isinstance(x, pd.core.generic.NDFrame)
    elif mod == "builtins":
        return isinstance(x, (str, bytes))
    else:
        return False


def dumps(x):
    """ Manage between cloudpickle and pickle

    1.  Try pickle
    2.  If it is short then check if it contains __main__
    3.  If it is long, then first check type, then check __main__
    """
    try:
        result = pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
        if len(result) < 1000:
            if b"__main__" in result:
                return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
            else:
                return result
        else:
            if _always_use_pickle_for(x) or b"__main__" not in result:
                return result
            else:
                return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        try:
            return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as e:
            logger.info("Failed to serialize %s. Exception: %s", x, e)
            raise


def loads(x):
    try:
        return pickle.loads(x)
    except Exception:
        logger.info("Failed to deserialize %s", x[:10000], exc_info=True)
        raise
