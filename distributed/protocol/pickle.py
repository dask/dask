from __future__ import print_function, division, absolute_import

import logging
import pickle

import cloudpickle

from ..utils import ignoring

logger = logging.getLogger(__file__)

pickle_types = [str, bytes]
with ignoring(ImportError):
    import numpy as np
    pickle_types.append(np.ndarray)
with ignoring(ImportError):
    import pandas as pd
    pickle_types.append(pd.core.generic.NDFrame)
pickle_types = tuple(pickle_types)


def dumps(x):
    """ Manage between cloudpickle and pickle

    1.  Try pickle
    2.  If it is short then check if it contains __main__
    3.  If it is long, then first check type, then check __main__
    """
    try:
        result = pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
        if len(result) < 1000:
            if b'__main__' in result:
                return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
            else:
                return result
        else:
            if isinstance(x, pickle_types) or b'__main__' not in result:
                return result
            else:
                return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
    except:
        try:
            return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            logger.info("Failed to serialize %s", x, exc_info=True)
            raise


def loads(x):
    try:
        return pickle.loads(x)
    except Exception:
        logger.info("Failed to deserialize %s", x[:10000], exc_info=True)
        raise
