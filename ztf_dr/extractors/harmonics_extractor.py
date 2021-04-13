import numpy as np
import pandas as pd

from lc_classifier.features.extractors import HarmonicsExtractor
from typing import List
from ztf_dr.extractors.base import DR_base


class DRHarmonicsExtractor(DR_base, HarmonicsExtractor):
    def __init__(self):
        super(DR_base, self).__init__()
        super(DRHarmonicsExtractor, self).__init__()

    def get_required_keys(self) -> List[str]:
        return ["hmjd", "mag", "magerr"]

    def _compute(self, light_curve: pd.DataFrame, **kwargs) -> pd.Series:
        objectid = light_curve.name
        magnitude = light_curve['mag']
        time = light_curve['hmjd']
        error = light_curve['magerr'] + 10 ** -2
        period = self._get_period(objectid, **kwargs)

        try:
            best_freq = 1 / period
            omega = [np.array([[1.] * len(time)])]
            timefreq = (2.0 * np.pi * best_freq * np.arange(1, self.n_harmonics + 1)).reshape(1, -1).T * time
            omega.append(np.cos(timefreq))
            omega.append(np.sin(timefreq))

            # Omega.shape == (lc_length, 1+2*self.n_harmonics)
            omega = np.concatenate(omega, axis=0).T

            inverr = 1.0 / error

            # weighted regularized linear regression
            w_a = inverr.reshape(-1, 1) * omega
            w_b = (magnitude * inverr).reshape(-1, 1)
            coeffs = np.matmul(np.linalg.pinv(w_a), w_b).flatten()
            fitted_magnitude = np.dot(omega, coeffs)
            coef_cos = coeffs[1:self.n_harmonics + 1]
            coef_sin = coeffs[self.n_harmonics + 1:]
            coef_mag = np.sqrt(coef_cos ** 2 + coef_sin ** 2)
            coef_phi = np.arctan2(coef_sin, coef_cos)

            # Relative phase
            coef_phi = coef_phi - coef_phi[0] * np.arange(1, self.n_harmonics + 1)
            coef_phi = coef_phi[1:] % (2 * np.pi)

            mse = np.mean((fitted_magnitude - magnitude) ** 2)
            out = pd.Series(
                data=np.concatenate([coef_mag, coef_phi, np.array([mse])]),
                index=self.get_features_keys())
            return out

        except Exception as e:
            return self.nan_series()
