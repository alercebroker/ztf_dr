import celerite2
import numpy as np
import pandas as pd

from lc_classifier.features.extractors import GPDRWExtractor
from typing import List
from scipy.optimize import minimize
from ztf_dr.extractors.base import DR_base


def set_params(params, gp, time, sq_error):
    gp.mean = 0.0
    theta = np.exp(params)
    gp.kernel = celerite2.terms.RealTerm(a=theta[0], c=theta[1])
    gp.compute(time, diag=sq_error, quiet=True)
    return gp


def neg_log_like(params, gp, time, mag, sq_error):
    gp = set_params(params, gp, time, sq_error)
    return -gp.log_likelihood(mag)


class DRGPDRWExtractor(DR_base, GPDRWExtractor):
    def __init__(self):
        super(DR_base).__init__()
        super(DRGPDRWExtractor, self).__init__()

    def get_required_keys(self) -> List[str]:
        return ['hmjd', 'mag', 'fid', 'magerr']

    def _compute(self, light_curve: pd.DataFrame, **kwargs) -> pd.Series:
        time = light_curve["hmjd"]
        mag = light_curve["mag"]
        err = light_curve["magerr"]

        time = time - time.min()
        mag = mag - mag.mean()
        sq_error = err ** 2

        kernel = celerite2.terms.RealTerm(a=1.0, c=10.0)
        gp = celerite2.GaussianProcess(kernel, mean=0.0)

        initial_params = np.array([np.log(1.0), np.log(1.0)])
        sol = minimize(
            neg_log_like,
            initial_params,
            method="L-BFGS-B",
            args=(gp, time, mag, sq_error))

        optimal_params = np.exp(sol.x)
        out_data = [
            optimal_params[0],
            1.0 / optimal_params[1]
        ]

        out = pd.Series(data=out_data, index=self.get_features_keys())
        return out
