import numpy as np

def check_limit(data):
    """
    Return the number of elements more than ±3 standard deviations from the mean.

    Parameters
    ----------
    data : array-like
        A 1-D (or any shape) sequence of numeric values.

    Returns
    -------
    int
        How many points satisfy |x – μ| > 3σ.
    """
    x       = np.asarray(data, dtype=float)
    mu      = x.mean()
    sigma   = x.std()           # population σ (ddof=0)
    mask    = np.abs(x - mu) > 3 * sigma
    return int(mask.sum())

