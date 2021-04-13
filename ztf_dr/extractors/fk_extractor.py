import pandas as pd
import numpy as np

from lc_classifier.features import FoldedKimExtractor
from typing import List
from ztf_dr.extractors.base import DR_base


class DRFoldedKimExtractor(DR_base, FoldedKimExtractor):
    def __init__(self):
        super(DR_base, self).__init__()
        super(DRFoldedKimExtractor, self).__init__()

    def get_required_keys(self) -> List[str]:
        return ['hmjd', 'mag']

    def _compute(self, light_curve: pd.DataFrame, **kwargs) -> pd.Series:
        objectid = light_curve.name
        period = self._get_period(objectid, **kwargs)
        times = light_curve["hmjd"]

        folded_time = np.mod(times, 2 * period) / (2 * period)
        magnitude = light_curve["mag"]
        sorted_mags = magnitude[np.argsort(folded_time)]
        sigma = np.std(sorted_mags)
        m = np.mean(sorted_mags)
        lc_len = len(sorted_mags)
        s = np.cumsum(sorted_mags - m) * 1.0 / (lc_len * sigma)
        psi_cumsum = np.max(s) - np.min(s)
        sigma_squared = sigma ** 2
        psi_eta = (1.0 / ((lc_len - 1) * sigma_squared) * np.sum(np.power(sorted_mags[1:] - sorted_mags[:-1], 2)))

        out = pd.Series(data=[psi_cumsum, psi_eta], index=self.get_features_keys())
        return out
