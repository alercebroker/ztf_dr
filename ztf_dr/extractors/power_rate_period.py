import numpy as np
import pandas as pd

from lc_classifier.features.extractors import PowerRateExtractor
from typing import List
from ztf_dr.extractors.base import DR_base


class DRPowerRateExtractor(DR_base, PowerRateExtractor):
    def __init__(self):
        super(DRPowerRateExtractor, self).__init__(bands=[0])
        super(DR_base, self).__init__(bands=[0])

    def get_required_keys(self) -> List[str]:
        return ["hmjd", "mag", "magerr", "filterid"]

    def _compute(self, light_curve: pd.DataFrame, **kwargs) -> pd.Series:
        objectid = light_curve.name
        periodogram = self._get_periodogram(objectid, **kwargs)
        if isinstance(periodogram["freq"], np.ndarray):
            power_rate_values = []
            for factor in self.factors:
                power_rate_values.append(self._get_power_ratio(periodogram, factor))
            return pd.Series(data=power_rate_values, index=self.get_features_keys())
        else:
            return self.nan_series()
