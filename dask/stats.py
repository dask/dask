# Copyright 2002 Gary Strangman.  All rights reserved
# Copyright 2002-2016 The SciPy Developers
#
# The original code from Gary Strangman was heavily adapted for
# use in SciPy by Travis Oliphant.  The original code came with the
# following disclaimer:
#
# This software is provided "as-is".  There are no expressed or implied
# warranties of any kind, including, but not limited to, the warranties
# of merchantability and fitness for a given application.  In no event
# shall Gary Strangman be liable for any direct, indirect, incidental,
# special, exemplary or consequential damages (including, but not limited
# to, loss of use, data or profits, or business interruption) however
# caused and on any theory of liability, whether in contract, strict
# liability or tort (including negligence or otherwise) arising in any way
# out of the use of this software, even if advised of the possibility of
# such damage.

import numpy as np
import dask.array as da
from dask.array.random import doc_wraps
from dask.array.ufunc import wrap_elemwise
from dask import delayed
import scipy.stats
from scipy.stats import distributions
from scipy import special
from scipy.stats.stats import (Ttest_indResult, Ttest_1sampResult,
                               Ttest_relResult, MannwhitneyuResult,
                               Power_divergenceResult)

# -----------------
# Statistical Tests
# -----------------


def _equal_var_ttest_denom(v1, n1, v2, n2):
    df = n1 + n2 - 2.0
    svar = ((n1 - 1) * v1 + (n2 - 1) * v2) / df
    denom = da.sqrt(svar * (1.0 / n1 + 1.0 / n2))  # XXX: np -> da
    return df, denom


def _unequal_var_ttest_denom(v1, n1, v2, n2):
    vn1 = v1 / n1
    vn2 = v2 / n2
    with np.errstate(divide='ignore', invalid='ignore'):
        df = (vn1 + vn2)**2 / (vn1**2 / (n1 - 1) + vn2**2 / (n2 - 1))

    # If df is undefined, variances are zero (assumes n1 > 0 & n2 > 0).
    # Hence it doesn't matter what df is as long as it's not NaN.
    df = da.where(da.isnan(df), 1, df)  # XXX: np -> da
    denom = da.sqrt(vn1 + vn2)
    return df, denom


def _ttest_ind_from_stats(mean1, mean2, denom, df):

    d = mean1 - mean2
    with np.errstate(divide='ignore', invalid='ignore'):
        t = da.divide(d, denom)  ###: np -> da
    t, prob = _ttest_finish(df, t)

    return (t, prob)


def _ttest_finish(df, t):
    """Common code between all 3 t-test functions."""
    # XXX: np.abs -> da.absolute
    # XXX: delayed(distributions.t.sf)
    prob = delayed(distributions.t.sf)(da.absolute(t),
                                       df) * 2  # use np.abs to get upper tail
    if t.ndim == 0:
        t = t[()]

    return t, prob


@doc_wraps(scipy.stats.ttest_ind)
def ttest_ind(a, b, axis=0, equal_var=True):
    v1 = da.var(a, axis, ddof=1)  # XXX: np -> da
    v2 = da.var(b, axis, ddof=1)  # XXX: np -> da
    n1 = a.shape[axis]
    n2 = b.shape[axis]

    if equal_var:
        df, denom = _equal_var_ttest_denom(v1, n1, v2, n2)
    else:
        df, denom = _unequal_var_ttest_denom(v1, n1, v2, n2)

    res = _ttest_ind_from_stats(da.mean(a, axis), da.mean(b, axis), denom, df)

    return Ttest_indResult(*res)


@doc_wraps(scipy.stats.ttest_1samp)
def ttest_1samp(a, popmean, axis=0, nan_policy='propagate'):
    # TODO: NaN handling
    n = a.shape[axis]
    df = n - 1

    d = da.mean(a, axis) - popmean
    v = da.var(a, axis, ddof=1)
    denom = da.sqrt(v / float(n))

    with np.errstate(divide='ignore', invalid='ignore'):
        t = da.divide(d, denom)
    t, prob = _ttest_finish(df, t)
    return Ttest_1sampResult(t, prob)


@doc_wraps(scipy.stats.ttest_rel)
def ttest_rel(a, b, axis=0, nan_policy='propagate'):
    # TODO: NaN handling

    if a.shape[axis] != b.shape[axis]:
        raise ValueError('unequal length arrays')

    if a.size == 0 or b.size == 0:
        return np.nan, np.nan

    n = a.shape[axis]
    df = float(n - 1)

    d = (a - b).astype(np.float64)
    v = da.var(d, axis, ddof=1)
    dm = da.mean(d, axis)
    denom = da.sqrt(v / float(n))

    with np.errstate(divide='ignore', invalid='ignore'):
        t = da.divide(dm, denom)
    t, prob = _ttest_finish(df, t)

    return Ttest_relResult(t, prob)


# @doc_wraps(scipy.stats.mannwhitneyu)
# def mannwhitneyu(x, y, use_continuity=True, alternative=None):
#     # https://en.wikipedia.org/wiki/Mann%E2%80%93Whitney_U_test#Calculations
#     # first method seems more doable?
#     # second method relies on ranking
#     result = (x > y).astype(float)
#     result[x == y] = 0.5
#     u1 = result.sum()
#     u2 = (1 - result).sum()
#     return u1, u2

#     n1 = len(x)
#     n2 = len(y)
#     ranked = rankdata(np.concatenate((x, y)))
#     rankx = ranked[0:n1]  # get the x-ranks
#     u1 = n1 * n2 + (n1 * (n1 + 1)) / 2.0 - np.sum(
#         rankx, axis=0)  # calc U for x
#     u2 = n1 * n2 - u1  # remainder is U for y
#     T = tiecorrect(ranked)
#     if T == 0:
#         raise ValueError('All numbers are identical in mannwhitneyu')
#     sd = np.sqrt(T * n1 * n2 * (n1 + n2 + 1) / 12.0)

#     meanrank = n1 * n2 / 2.0 + 0.5 * use_continuity
#     if alternative is None or alternative == 'two-sided':
#         bigu = max(u1, u2)
#     elif alternative == 'less':
#         bigu = u1
#     elif alternative == 'greater':
#         bigu = u2
#     else:
#         raise ValueError("alternative should be None, 'less', 'greater' "
#                          "or 'two-sided'")

#     z = (bigu - meanrank) / sd
#     if alternative is None:
#         # This behavior, equal to half the size of the two-sided
#         # p-value, is deprecated.
#         p = distributions.norm.sf(abs(z))
#     elif alternative == 'two-sided':
#         p = 2 * distributions.norm.sf(abs(z))
#     else:
#         p = distributions.norm.sf(z)

#     u = u2
#     # This behavior is deprecated.
#     if alternative is None:
#         u = min(u1, u2)
#     return MannwhitneyuResult(u, p)



@doc_wraps(scipy.stats.chisquare)
def chisquare(f_obs, f_exp=None, ddof=0, axis=0):
    return power_divergence(f_obs, f_exp=f_exp, ddof=ddof, axis=axis,
                            lambda_="pearson")


@doc_wraps(scipy.stats.power_divergence)
def power_divergence(f_obs, f_exp=None, ddof=0, axis=0, lambda_=None):

    if isinstance(lambda_, str):
        # TODO: public api
        if lambda_ not in scipy.stats.stats._power_div_lambda_names:
            names = repr(list(scipy.stats.stats._power_div_lambda_names.keys()))[1:-1]
            raise ValueError("invalid string for lambda_: {0!r}.  Valid strings "
                             "are {1}".format(lambda_, names))
        lambda_ = scipy.stats.stats._power_div_lambda_names[lambda_]
    elif lambda_ is None:
        lambda_ = 1

    if f_exp is not None:
        # f_exp = np.atleast_1d(np.asanyarray(f_exp))
        pass
    else:
        f_exp = f_obs.mean(axis=axis, keepdims=True)

    # `terms` is the array of terms that are summed along `axis` to create
    # the test statistic.  We use some specialized code for a few special
    # cases of lambda_.
    if lambda_ == 1:
        # Pearson's chi-squared statistic
        terms = (f_obs - f_exp)**2 / f_exp
    elif lambda_ == 0:
        # Log-likelihood ratio (i.e. G-test)
        terms = 2.0 * _xlogy(f_obs, f_obs / f_exp)
    elif lambda_ == -1:
        # Modified log-likelihood ratio
        terms = 2.0 * _xlogy(f_exp, f_exp / f_obs)
    else:
        # General Cressie-Read power divergence.
        terms = f_obs * ((f_obs / f_exp)**lambda_ - 1)
        terms /= 0.5 * lambda_ * (lambda_ + 1)

    stat = terms.sum(axis=axis)

    num_obs = _count(terms, axis=axis)
    # ddof = asarray(ddof)
    p = delayed(distributions.chi2.sf)(stat, num_obs - 1 - ddof)

    return Power_divergenceResult(stat, p)

def _count(x, axis=None):
    if axis is None:
        return x.size
    else:
        return x.shape[axis]


# Don't really want to do all of scipy.special (or do we?)
_xlogy = wrap_elemwise(special.xlogy)

# -------------
# Distributions

# - rvs should return dask.arrays
# - pdf / pmf / cdf / sf / isf / logpdf /lodgcdf / logpmf / logsf should be delayed
