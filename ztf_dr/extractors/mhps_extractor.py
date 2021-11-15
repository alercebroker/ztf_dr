import mhps
import pandas as pd

from ztf_dr.extractors.base import DR_base
from lc_classifier.features import MHPSExtractor
from typing import List, Tuple


class DRMHPSExtractor(DR_base, MHPSExtractor):
    def __init__(self, t1=100, t2=10, dt=3.0, mag0=19.0, epsilon=1.0):
        super().__init__(t1=t1, t2=t2, dt=dt, mag0=mag0, epsilon=epsilon, bands=[0])

    def get_required_keys(self) -> List[str]:
        return ['hmjd', 'mag', 'magerr']

    def get_features_keys(self) -> Tuple[str, ...]:
        return self.get_features_keys_without_band()

    def _compute(self, light_curve, **kwargs) -> pd.Series:
        mag = light_curve["mag"]
        magerr = light_curve["magerr"]
        time = light_curve["hmjd"]
        ratio, low, high, non_zero, pn_flag = mhps.statistics(mag, magerr, time, self.t1, self.t2)
        values = {
            'MHPS_ratio': ratio,
            'MHPS_low': low,
            'MHPS_high': high,
            'MHPS_non_zero': non_zero,
            'MHPS_PN_flag': pn_flag
        }
        return pd.Series(values)
